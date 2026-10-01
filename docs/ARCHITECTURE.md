# Architecture

This file records decisions that are not obvious from reading the code, in the order
they were made, with what was verified at each checkpoint. The scope and data contract
are in `CLAUDE.md`.

## Overview

Synthetic Debezium-format events for two tables, `accounts` and `journal_entries`, land
in S3. One pipeline, `gl_pipeline`, reads them into bronze with Auto Loader, applies
them to silver with AUTO CDC sequenced by `source.lsn`, and builds two gold
materialized views. Jobs around the pipeline schedule it, check freshness and the trial
balance, replay or full-refresh on request, and apply governance. Terraform owns the AWS
and Unity Catalog resources; the Asset Bundle owns the pipeline and jobs, and CI
deploys it as the `gl-cicd` service principal. The README has the diagram.

Contents:

- [Checkpoint 1: Terraform and generator](#checkpoint-1-terraform-and-generator), and
  the [generator](#generator)
- [Checkpoint 2: Bronze and silver CDC](#checkpoint-2-bronze-and-silver-cdc)
- [Checkpoint 3: Quality and gold](#checkpoint-3-quality-and-gold)
- [Checkpoint 4: Governance](#checkpoint-4-governance)
- [Checkpoint 5: Reliability](#checkpoint-5-reliability)
- [Checkpoint 6: Tuning, cost, CI/CD](#checkpoint-6-tuning-cost-cicd)

Tuning and cost numbers are in `docs/results.md`, operating steps in `docs/RUNBOOK.md`.

## Checkpoint 1: Terraform and generator

### One bucket, one prefix per environment

`s3://gl-cdc-lakehouse-anuj/dev/cdc/...` and `.../prod/cdc/...` share a bucket, a KMS
key, an IAM role and a storage credential. Two buckets would isolate the environments
better, but this is a portfolio project on a personal AWS account and each customer
managed KMS key costs about a dollar a month whether or not it is used. The isolation
that matters is still there: each environment has its own external location and its own
catalog, so a Unity Catalog grant in dev cannot reach prod data.

The cost of that choice is that both environments live in one Terraform state, because
they share resources that only one state can own. So the environment list is a variable
rather than the state boundary:

- `envs/dev.tfvars` sets `environments = ["dev"]`
- `envs/prod.tfvars` sets `environments = ["dev", "prod"]`

Applying the prod var-file adds prod resources and leaves dev untouched. If the project
ever needs real environment isolation, the split is one bucket per environment and one
state per environment, and the module code does not change.

### The storage credential and IAM role cycle

Unity Catalog generates the external ID when the storage credential is created, and
that external ID has to appear in the IAM role's trust policy. So the credential must
exist before the role, but the credential also names the role. Writing that directly
gives Terraform a dependency cycle.

The break is to give the credential a role ARN built as a string from the account ID
and the role name, rather than a reference to `aws_iam_role.uc`:

```hcl
uc_role_arn = "arn:aws:iam::${account_id}:role/${local.uc_role_name}"
```

Now the dependency runs one way: credential, then trust policy, then role. The
credential is created with `skip_validation = true` because at that moment the role
does not exist yet. Validation is not skipped overall: the external location is created
after the role and does validate, so a broken credential still fails the apply.

The trust policy comes from the `databricks_aws_unity_catalog_assume_role_policy` data
source, which emits both required statements: the cross-account grant to the Databricks
Unity Catalog master role under the external ID condition, and the self-assume
statement that AWS has required since June 2023 and Databricks has enforced since
January 2025.

`time_sleep.iam_propagation` holds for 30 seconds between creating the role and
validating the external location. IAM is eventually consistent, and without the pause
the first apply can fail on a role that is not assumable yet.

### Each catalog names its own managed storage location

This account has Default Storage enabled and the metastore has no storage root, so a
catalog created without `storage_root` is rejected. Each catalog therefore points at
`s3://gl-cdc-lakehouse-anuj/<env>/managed`, and that path needs an external location of
its own, `gl_<env>_managed`, because a managed location has to sit inside one.

So there are two external locations per environment:

| Location | Path | Used for |
|---|---|---|
| `gl_<env>_landing` | `<env>/cdc` | Auto Loader reading raw CDC JSON |
| `gl_<env>_managed` | `<env>/managed` | Delta files behind the managed tables |

They are siblings, never nested. Unity Catalog refuses a managed location that overlaps
a path holding external tables, and keeping them apart also means a lifecycle rule aimed
at landing files cannot reach table storage.

The grants differ to match. Engineers get `READ_FILES`, `WRITE_FILES` and
`CREATE_EXTERNAL_TABLE` on the landing location, and only `CREATE_MANAGED_STORAGE` on
the managed one. Managed table files are reached through the tables, never as files.

Putting managed storage in our own bucket rather than Databricks Default Storage also
keeps the Delta files under the project KMS key, which is what makes the PII governance
in checkpoint 4 hold all the way down to the object store.

### Grants

Analysts get `USE_CATALOG` on the catalog and `USE_SCHEMA` plus `SELECT` on `gold`
only. They get nothing on bronze or silver, so the PII columns there are unreachable
regardless of masking. Engineers get the schema privileges they need to build tables,
not `ALL PRIVILEGES`. Catalogs and schemas are owned by the `gl_engineers` group rather
than by one person, so ownership survives a change of maintainer.

Masks, row filters and column tags are checkpoint 4 and live in `sql/governance/`.

## Generator

### Time is simulated, not slept through

`--minutes 10` covers the ten minutes ending now, one file per table per simulated
minute, all uploaded immediately. A ten minute run takes seconds. Files land in current
and recent `dt=`/`hh=` partitions, so Auto Loader sees them as new arrivals.

### Landing partitions use landing time, not event time

Files are keyed `cdc/<table>/dt=YYYY-MM-DD/hh=HH/part-<epoch>-<run>-<seq>.json`. A late
arriving event therefore sits in a fresh partition while carrying an old `lsn`. That is
exactly the case bronze and silver have to handle, and partitioning by event time would
hide it.

### lsn follows wall-clock time

`lsn` is the run's time in milliseconds times 100, plus a counter. Real log sequence
numbers only grow, across runs as well as within one. The first version started every
run at 5,000,000, so two runs landed different rows under the same (key, lsn), which no
ordering can resolve (see `AI_USAGE.md`). Anchoring to time means a later run always
sequences after an earlier one: a run's events use at most 64 slots each, far fewer than
the 100 slots per millisecond between runs.

The anchor is the run's actual start time. Until checkpoint 3 it was the window end,
rounded down to the minute, so two runs started in the same minute began at the same
lsn.

### Ids are unique per run, except the base accounts

Each run has a run tag, its start time in milliseconds. Journal ids
(`JRN-<run>-<n>`), entry ids (`JE-<run>-<n>`), accounts opened during the run
(`ACC-<run>-<n>`) and landing file names all carry it. The base accounts,
`ACC-000001` to `ACC-000200`, keep the same ids in every run: each run re-reads the same
accounts, the way a real source would.

Until checkpoint 3 journal and entry ids were counters that restarted at 1 every run. A
new run therefore overwrote the previous run's journal lines by key instead of adding
to them (see `AI_USAGE.md`). File names had the same problem: two runs started in the
same minute wrote the same S3 keys, replacing files the pipeline had already ingested.

One consequence: the held-back "late" accounts are base accounts, so they are only
really late on an empty landing prefix. On a prefix that already has a run, they exist
in silver before their late create arrives, and the create just adds a version.

### Late arrivals get a genuinely older lsn

A late event gets the lsn a change made at its backdated time would have had, up to
`--late-hours` (48 by default) in the past, and a matching `ts_ms`. So a late event is
not just old by timestamp, it is old by the sequence key that silver actually orders on.
Three accounts are also held back so their journal entries land before the account
exists, which exercises the warn-only orphan foreign key rule.

### Journals balance within a business unit and currency

The data contract says debits equal credits per `journal_id`. The `trial_balance_check`
job is stricter: it nets debits against credits per business unit, currency and entry
date. A journal whose lines crossed two business units would satisfy the contract and
still fail the job. So every line of a journal is drawn from accounts that share a
business unit and a currency, and updates only touch free text. Deletes remove a whole
journal, never a single line.

### Amounts are integer cents internally

Amounts are held as integers and formatted as a two decimal place string on the way
out. Floats would drift and break the balance invariant before the data ever reached
Spark.

### SSE-KMS comes from the bucket default

The generator sends no server side encryption headers. The bucket has default SSE-KMS
with the project key, so objects are encrypted without the generator needing the key
ID. Passing `ServerSideEncryption=aws:kms` without a key ID would quietly switch to the
AWS managed `aws/s3` key instead, which is not the key the Unity Catalog role is
granted on.

## Checkpoint 2: Bronze and silver CDC

### API names

Pipeline code uses the current Lakeflow Python API throughout: `from pyspark import
pipelines as dp`, `@dp.table`, `@dp.temporary_view`, `@dp.expect_all_or_drop`,
`dp.create_streaming_table` and `dp.create_auto_cdc_flow`. Not the older `dlt` module or
`apply_changes`.

### One pipeline, fully qualified names

`gl_pipeline` has `silver` as its default schema but writes every table by its full
name, `gl_<env>.bronze.*` and `gl_<env>.silver.*`. One pipeline keeps bronze and silver
in one update and one dependency graph. The catalog and landing path come in as pipeline
configuration (`gl.catalog`, `gl.landing_root`), both derived from the bundle's `env`.

`root_path` is `src/`, which the runtime puts on `sys.path`. Pipeline files therefore
import `transforms.cdc` the same way the unit tests do (`pythonpath = ["src"]` in
`pyproject.toml`).

### Bronze stays untyped

Bronze reads landing NDJSON with Auto Loader against an explicit schema where every row
field is a string. Unparseable lines keep their text in `_corrupt_record`, and
unexpected fields go to `_rescued_data`. Nothing is dropped or cast. A bad value can
never fail ingest, and silver can always be rebuilt from bronze.

### Silver: what goes into AUTO CDC

A temporary view per source table flattens the envelope, takes the row from `after` (or
`before` for deletes), casts with `try_cast`, drops rows that cannot be keyed or
sequenced, and removes exact duplicates. Three flows read from those views:

| Target | SCD type | Keys | Sequence | Deletes |
|---|---|---|---|---|
| `silver.accounts` | 1 | account_id | `_lsn` | removed |
| `silver.account_history` | 2 | account_id | `_lsn` | close the current version |
| `silver.journal_entries` | 1 | entry_id | `_lsn` | removed |

`try_cast` matters because serverless runs with ANSI mode on, where a plain cast of a
bad string fails the whole batch. A bad value becomes null and is left for the
checkpoint 3 quarantine rules.

The drop rules each test for NULL explicitly (`_op IS NOT NULL AND _op IN (...)`), so an
expectation never evaluates to NULL, which is neither a clear pass nor a clear fail.

### Duplicates are removed before AUTO CDC

The AUTO CDC docs do not say what happens when two events share a key and a sequence
value. Rather than depend on undocumented behaviour, the source view removes them with
`dropDuplicatesWithinWatermark` on (key, lsn) and a one hour watermark on
`_ingested_at`. Since lsn is unique per change, same key and lsn means the same change
delivered twice. The generator writes duplicates into the same file, so they arrive in
the same micro-batch, and the watermark keeps dedupe state from growing forever.

### Tombstones kept for seven days

When AUTO CDC applies a delete to an SCD1 table, it keeps the key as a tombstone so a
later-arriving, older event for that key is recognised as stale. The tombstones are
garbage collected after `pipelines.cdc.tombstoneGCThresholdInSeconds`, two days by
default. The generator's late arrivals are up to 48 hours old, which leaves no room for
the gap before the next scheduled run or for a backfill. After collection, a stale
update would re-insert a deleted row. Both SCD1 tables set the threshold to seven days.

### The reference state is the test oracle

AUTO CDC does not run outside Databricks, so the unit tests cannot exercise it directly.
Instead `transforms/cdc.py` holds a batch reference of what AUTO CDC should produce:

- SCD1: the highest lsn per key wins, and if that event is a delete the row is gone.
- SCD2: every non-delete event opens a version at its lsn; the next event for the same
  key closes it.

The unit tests pin the reference down on hand-made cases (out of order, duplicates,
deletes, late arrivals, update before a late create, update after delete) and on real
generator output. The `silver_state_check` job then runs the same reference over the
landing files in S3, not over bronze, and compares it to the silver tables row for row
with `exceptAll`. If AUTO CDC and the reference disagree, one of them is wrong and the
check says which rows.

The job also prints an order-independent checksum per table (sum of `xxhash64` per
row). Running it after an update and again after a full refresh proves the rebuild is
idempotent.

In this checkpoint silver still contains rows that checkpoint 3 will quarantine (for
example a null amount). The reference keeps them too, so the comparison holds.

### Verified on dev

Run on 2026-09-29 by Anuj, after clearing the old landing files. Generator:
`--minutes 10 --rate 50`, default seed 42. Steps: deploy, generate, pipeline update,
`silver_state_check`, full refresh, `silver_state_check` again.

| Table | Rows | Missing | Extra | Checksum |
|---|---|---|---|---|
| accounts | 204 | 0 | 0 | 91396765124063505071 |
| account_history | 280 | 0 | 0 | -125919159872533020634 |
| journal_entries | 410 | 0 | 0 | -198654095462340390530 |

- AUTO CDC and the reference agree row for row on all three tables, SCD2 history
  included.
- The full refresh produced the same checksums, so rebuilding silver from the same
  landing files gives identical tables.
- Both things the docs left open held up in practice. The unparseable lines did not fail
  the Auto Loader read, and since silver matches a reference that drops them, they did
  not reach silver. The check job runs on serverless environment version 5.

## Checkpoint 3: Quality and gold

### Every event goes exactly one way

Per source table, silver routes each flattened event in this order:

1. Unparseable (`_corrupt_record` is set): appended to `silver.quarantine_events` with
   reason `unparseable_payload`.
2. A drop rule fails (missing PK, null lsn, unknown op): dropped by
   `expect_all_or_drop`, counted in the event log.
3. A quarantine rule fails: appended to `silver.quarantine_events` with the names of
   every rule it failed.
   - `amount_positive`: `amount IS NOT NULL AND amount > 0` (journal_entries)
   - `currency_known`: currency in the allowlist (both tables)
4. Otherwise: into AUTO CDC.

The order matters in one place. An unparseable line has no key, so if the drop rules ran
first it would fail `pk_present` and vanish with only a count to show for it. Splitting
unparseable rows off first means the raw text is kept where someone can read it.

In the pipeline this is three temporary views per table (`_parsed`, `_checked`,
`_valid`) and two append flows into `quarantine_events` (`_unparseable`, `_rejected`).
Two flows rather than one union because the checked side carries a watermark from the
dedupe and the parsed side does not. The quarantine rules also run as warn-only
`expect_all` on the checked view, so their failure counts sit in the event log next to
the drop counts.

A NULL amount is quarantined, not passed through. It only happens when `try_cast`
failed, and a ledger line with no amount is as unusable as a negative one.

`quarantine_events` has one schema for both tables: `source_table`, `reasons`,
`payload` (the unparseable line itself, or the envelope as JSON), `_op`, `_lsn`,
`_ingested_at`, `_source_file`.

### Unparseable rows skip the dedupe

The dedupe keys on (pk, lsn). Both are null on an unparseable row, so every one of them
would count as a duplicate of the others and collapse into a single row. `cdc.check`
filters them out before the dedupe, and a unit test sends five distinct corrupt lines
and expects five quarantine rows.

### Currency is an allowlist

`CURRENCIES = ("USD", "EUR", "GBP", "JPY")` in `transforms/cdc.py`, the currencies the
ledger books in. A full ISO 4217 list would let the generator's `XXX` through, because
`XXX` is a real ISO code meaning "no currency". A unit test keeps the allowlist equal to
the generator's list.

### Gold is two materialized views

| Table | Grain | Columns |
|---|---|---|
| `gold.account_balances` | account, currency | line_count, debit_total, credit_total, balance |
| `gold.daily_trial_balance` | business_unit, currency, entry_date | line_count, debit_total, credit_total, net |

Materialized views rather than streaming tables, because both are aggregates over SCD1
tables that change in place, and a late account changes groups that were already
aggregated. A materialized view is always its query over current silver, so a late
account reconciles on the next update with no extra logic. The aggregation lives in
`transforms/gold.py`, and the pipeline, the unit tests and the state check all call it.

`balance` follows the account type's normal side: debits minus credits for ASSET and
EXPENSE, credits minus debits for LIABILITY, EQUITY and REVENUE. A healthy liability
reads as positive, the way a ledger does. `net` in the trial balance is always debits
minus credits and is zero when the books balance.

### Account attributes come from the latest history version

Gold takes business_unit and account_type from the newest version of each account in
`silver.account_history`, not from `silver.accounts`. The generator deletes accounts
whose journal lines stay live. Joined to `silver.accounts`, those lines would lose
their business unit forever and the trial balance for that unit would never net to
zero. The closed SCD2 version still records the unit. The generator also changes
account_type on updates, so the latest version decides the sign of the balance.

### Orphan lines stay in gold

A journal line whose account has never landed is kept through a left join, with a NULL
business_unit and account_type. Its balance is NULL too, because the sign depends on a
type nobody knows yet. Nothing is dropped, so gold never loses money quietly.
`account_balances` carries a warn-only `account_known` expectation that counts these
rows.

While an account is late, the trial balance shows it: the NULL group has a non-zero
net, and so does the account's real unit. Once the account lands, the next update puts
the lines under the right unit and both groups net to zero.

### Proving reconciliation across two updates

In a normal generator run the held-back accounts land later in the same run, so a
single pipeline update sees them together with their journal lines. Two flags split
that into two runs:

- `--withhold-late-accounts` writes everything except the three late accounts. They get
  no create, update or delete, because an update would create the row.
- `--only-late-accounts`, with the same `--seed` and `--accounts`, writes only their
  create events. The same seed picks the same accounts, because everything before
  that choice draws from the seeded generator the same way in every mode.

### The state check covers quarantine and gold

`silver_state_check` compares silver, `quarantine_events` and both gold tables with the
reference computed from the landing files. Quarantine is compared without
`_ingested_at` and `_source_file`, which change on every rebuild. The check also prints
quarantine counts per reason and the number of unbalanced and orphan trial balance
groups.

Spark refuses a query on raw JSON files that reads only `_corrupt_record`. A count of
the unparseable reference rows is such a query, and serverless does not support
`cache()`, the usual way around it. So the check never counts the expected frame
directly: it derives the expected count as actual minus extra plus missing, from
`exceptAll` results, which read every column. Bronze is a Delta table, so the pipeline
itself is not affected.

Open source Spark 4.0.1 (the local test version) fails with an `INTERNAL_ERROR` when
`exceptAll` runs over the SCD1 reference (a window with a `rank = 1` filter). The same
code ran on Databricks in checkpoint 2, and the checkpoint 2 version of `cdc.py` fails
the same way locally, so this is a local Spark problem and not a checkpoint 3 change.
The unit tests do not use `exceptAll`.

### Verified on dev

**First run, 2026-09-29, by Anuj.** Steps: deploy, full refresh, generator with
`--withhold-late-accounts`, update, `silver_state_check`, generator with
`--only-late-accounts`, update, `silver_state_check`, full refresh,
`silver_state_check`. The landing prefix still held the checkpoint 2 run.

| Table | Rows | Missing | Extra | Checksum after follow-up and after full refresh |
|---|---|---|---|---|
| silver.accounts | 203 | 0 | 0 | -74444556911398783622 |
| silver.account_history | 558 | 0 | 0 | -126861808956568470557 |
| silver.journal_entries | 415 | 0 | 0 | -176125579209857999422 |
| silver.quarantine_events | 7 | 0 | 0 | 14739086700574879462 |
| gold.account_balances | 148 | 0 | 0 | 32754967820805642471 |
| gold.daily_trial_balance | 31 | 0 | 0 | -7434261366217672374 |

Quarantine reasons: accounts `currency_known` 2; journal_entries `amount_positive` 3,
`currency_known` 3, `unparseable_payload` 2. The trial balance had 0 unbalanced groups
in all three checks.

- Silver, quarantine and gold matched the reference row for row in all three checks.
- The checks before and after the full refresh have identical checksums, so a rebuild
  from the same landing files gives identical tables.
- Unparseable lines reached quarantine through Auto Loader and bronze with their text
  intact, which the unit tests could only show for a plain Spark read.
- Reconciliation was **not** proven. The first check already had 0 orphan groups,
  because the "late" accounts were base accounts that the checkpoint 2 run had landed.
  After the follow-up run `account_history` grew from 555 to 558 rows while `accounts`
  stayed at 203: the three late creates became extra versions of existing accounts.
  The run also showed journal ids restarting at 1 every run (415 journal lines after
  two full runs, 410 after one). Both are fixed in the generator, see "Ids are unique
  per run" above.

**Second run, 2026-09-29, by Anuj**, after the id fix. Steps: clear
`s3://gl-cdc-lakehouse-anuj/dev/cdc/`, generator with `--withhold-late-accounts`
(`--minutes 10 --rate 50`, seed 42), full refresh, check 1, generator with
`--only-late-accounts`, update, check 2, full refresh, check 3.

| Table | Check 1 rows | Check 2 rows | Check 2 and 3 checksum |
|---|---|---|---|
| silver.accounts | 200 | 203 | -70670424421286850505 |
| silver.account_history | 276 | 279 | 82629327533479081107 |
| silver.journal_entries | 415 | 415 | 34881271539060652540 |
| silver.quarantine_events | 3 | 3 | 1000157484841228543 |
| gold.account_balances | 148 | 148 | 32754967820805642471 |
| gold.daily_trial_balance | 36 | 31 | -7434261366217672374 |

Every table matched the reference with 0 missing and 0 extra in all three checks.
Quarantine reasons: accounts `currency_known` 1; journal_entries `amount_positive` 1,
`currency_known` 1, `unparseable_payload` 1.

| Trial balance | Check 1 | Check 2 | Check 3 |
|---|---|---|---|
| Unbalanced groups | 10 | 0 | 0 |
| Orphan groups (NULL business_unit) | 5 | 0 | 0 |

- **Late accounts reconcile.** Check 1 had 5 orphan groups holding the withheld
  accounts' lines, and 5 real business unit groups short by the same amounts, so 10
  unbalanced groups. After the three accounts landed, the next update moved their lines
  under the right unit and every group netted to zero. The 36 trial balance rows became
  31 because the 5 NULL groups merged into existing ones. `account_balances` kept 148
  rows: the orphan rows were the same accounts, now with their unit and type filled in.
- **Rebuilds are idempotent.** Checks 2 and 3 have identical checksums on every table,
  so a full refresh over the same landing files gives identical silver, quarantine and
  gold.
- These numbers match a local simulation of the same commands run beforehand (10
  unbalanced and 5 orphan groups, then 0 and 0).
- Both gold checksums after reconciliation equal the ones from the first run. Gold
  aggregates amounts per account, unit, currency and date, and holds no ids or
  timestamps. Both runs used seed 42 on the same day, so they produced the same amounts
  on the same accounts and dates. That fits the id fix: only ids changed between the
  runs, and gold does not see ids.

## Checkpoint 4: Governance

### Where each piece lives

| Piece | Where | Applied by |
|---|---|---|
| Catalog and schema grants | `infra/terraform/unity_catalog.tf` | Terraform |
| Mask and filter functions | `sql/governance/01_functions.sql` | `governance` job |
| Classification and pii tags | `sql/governance/02_tags.sql` | `governance` job |
| Masks and row filters | `sql/governance/03_masks_filters.sql` | `governance` job |
| EXECUTE on the functions | `sql/governance/04_grants.sql` | `governance` job |

The job is a serverless Python task that runs the files in name order through
`spark.sql`, so no SQL warehouse is needed. The files never name a catalog. The job
runs `USE CATALOG gl_<env>` first, so one set of files serves dev and prod.

Pipeline tables take `ALTER STREAMING TABLE` or `ALTER MATERIALIZED VIEW`. `ALTER TABLE`
is not supported on them. Bronze and silver are streaming tables and gold is two
materialized views.

### Masks and filters are attached by ALTER, not in the pipeline

They could also be declared in the pipeline code, with `row_filter=` and `MASK` in each
table's schema. Using `ALTER` keeps all governance in one directory, which is what
CLAUDE.md asks for. It also spares writing out a full schema for every AUTO CDC
target. This works because of a change in pipelines release 2025.29: pipeline updates
now keep masks and filters set with `ALTER`. Before that, each update removed them.

The cost: when the pipeline creates a table, the table is unprotected until the
governance job runs. On dev that window is accepted. The `backfill_replay` job in
checkpoint 5 runs governance after any full refresh. It is still unconfirmed whether a
full refresh keeps `ALTER`-set masks. That run will show it, and `governance_check`
fails if they are gone. (Answered in checkpoint 5: they are kept. See "Verified on dev"
there.)

### The pipeline owner must be in gl_pii_readers and gl_engineers

When a pipeline update refreshes a table, the mask and filter functions on the tables
it **reads** run with the pipeline owner's rights. When a person queries a table, they
run as that person. So the pipeline sees what its owner would see:

- Silver reads `bronze.accounts_cdc_raw`, which has masks on `before` and `after`. If
  the owner were not in `gl_pii_readers`, silver would be built from `REDACTED` and
  `********9012`, and the real values would be gone from silver.
- Gold reads `silver.account_history`, which has the row filter. If the owner were not
  in `gl_engineers`, gold would silently lose every unit the owner cannot see.

Both memberships were confirmed for the owner on 2026-09-29. `silver_state_check` runs
as the same identity, so it still compares full, unmasked values.

### Raw layers are masked too

The data contract names three PII columns, but the same values also sit in:

- `bronze.accounts_cdc_raw.before` and `.after` (the row structs)
- `_corrupt_record` and `_rescued_data` (raw text)
- `silver.quarantine_events.payload` (the event as JSON)

Analysts cannot reach bronze or silver. But without masks there, an engineer outside
`gl_pii_readers` could read every holder's email from bronze, and the silver masks
would protect nothing. So those columns carry `pii = true` and a mask as well:

- The struct mask returns the same struct type with the three PII fields masked.
  Unity Catalog requires a mask to return the column's exact type.
- `mask_payload` takes `source_table` as a second argument (`USING COLUMNS`). Only
  account payloads are redacted, so journal quarantine stays readable for debugging.

### Function design

- **`is_account_group_member`, not `is_member`.** The groups are account-level.
  `is_member` only checks workspace-local groups.
- **No function calls another UDF.** `mask_account_row` repeats the last-four rule
  instead of calling `mask_account_number`. The docs list "nesting" among the policy
  features that MERGE does not support, without saying exactly what counts. AUTO CDC
  writes silver with MERGE, so the functions stay flat. The first pipeline update after
  the governance job shows whether AUTO CDC can still write to the masked and filtered
  silver tables.
- **NULL stays NULL for everyone.** A missing value is not PII, and hiding it would
  hide data quality problems from engineers.
- **An account number of four characters or fewer is masked completely.** Otherwise
  its last four would be the whole value.
- **Rows with a NULL business_unit are visible to engineers only.** These are journal
  lines whose account has not landed. No analyst unit can claim them until it does.
- **`silver.journal_entries` has no row filter.** It has no business_unit column, and
  analysts have no silver access.

The unit tests run the real `01_functions.sql` on local Spark. Each function is created
as a temporary function beside a stub `is_account_group_member` that answers from a
fixed list of groups, so the tests exercise the SQL itself rather than a Python copy.

### Function ownership

Terraform gives the catalogs and schemas to `gl_engineers`. The functions are owned by
whoever first ran the governance job, because the SQL reference documents no
`ALTER FUNCTION ... OWNER TO` statement. `04_grants.sql` grants `EXECUTE` to
`gl_engineers` instead, so any engineer can attach the functions. Replacing a function
still needs its owner. If the maintainer changes, the owner is changed once in Catalog
Explorer.

### Pipeline backing tables

The pipeline keeps internal tables in the same schemas as the tables it publishes:

- a `__materialization_mat_<pipeline id>_<table>_1` table behind each streaming table
  and materialized view
- an `event_log_<pipeline id>` table in silver

They are hidden in Catalog Explorer but listed in `information_schema.tables`. The
backing tables hold the real data, unmasked and unfiltered: the one behind
`silver.accounts` has all three PII columns and no masks. The first `governance_check`
run found them, because it requires a classification tag on every silver and gold table.

Schema grants do not reach these tables. On dev on 2026-09-29, the Unity Catalog
effective permissions API returned:

| Principal | Table | Effective privileges |
|---|---|---|
| gl_analysts_treasury | gold.daily_trial_balance | SELECT, inherited from gl_dev.gold |
| gl_analysts_treasury | its gold backing table | none |
| gl_engineers | the silver accounts backing table | none |
| both groups | the event log | none |

So they are left untagged and unmasked, and `governance_check` skips them by name. The
toggle test confirmed it with real queries: as the test user, direct reads of two
backing tables failed with a permission error at every group step.

### Classification

| Classification | Tables | Why |
|---|---|---|
| confidential | bronze (both), silver accounts, account_history, quarantine_events | hold PII or raw events |
| confidential | silver journal_entries | the ledger at line level |
| internal | gold account_balances, daily_trial_balance | aggregates, no PII, filtered by unit |

### Verification

- `governance_check` reads `information_schema` and fails if any of the following is
  missing: a classification tag on a silver or gold table, a pii tag, a mask, or a row
  filter. It also fails if anyone holds `ALL PRIVILEGES` on the catalog or a schema.
- It shows what is attached, not what anyone sees. For that, a test user is moved
  through the groups and runs `tests/integration/governance_toggle.sql` at each step.

### Verified on dev

Run on 2026-09-29, in this order:

1. The `governance` job applied every file.
2. The first `governance_check` found the pipeline's internal tables in silver and gold
   (see "Pipeline backing tables"). After the check was changed to skip them, it passed:
   8 classifications, 11 masks with pii tags, 4 row filters, no `ALL PRIVILEGES`.
3. A generator run (448 journal events) and a pipeline update. AUTO CDC wrote to all
   three silver tables with the masks and row filter in place, and both gold views
   refreshed. The MERGE restriction did not apply to these flat functions.
4. `governance_check` passed again: the masks and filters survived the update.
5. `silver_state_check` matched on every table, 0 missing and 0 extra:

   | Table | Rows |
   |---|---|
   | silver.accounts | 206 |
   | silver.account_history | 558 |
   | silver.journal_entries | 823 |
   | silver.quarantine_events | 7 |
   | gold.account_balances | 153 |
   | gold.daily_trial_balance | 32 |

   The reference is computed from the landing files, PII columns included. A match
   means the pipeline read bronze unmasked, as its owner, and silver holds real values.
6. `governance` ran a second time, and `governance_check` still passed.

### Verified by group membership

A test user went through the groups one step at a time and ran
`tests/integration/governance_toggle.sql` at each step, on 2026-09-29, against the data
from the run above.

| Test user's groups | Gold units visible | trial balance rows / lines | account_balances rows | Silver and bronze PII |
|---|---|---|---|---|
| none | no access | - | - | no access |
| gl_analysts_treasury | TREASURY | 9 / 160 | 50 | no access |
| + gl_engineers | all three | 32 / 823 | 153 | masked |
| + gl_pii_readers | all three | 32 / 823 | 153 | clear |

- **Row filter.** The treasury analyst saw TREASURY only. With `gl_engineers` added,
  the totals equal the `silver_state_check` counts (32 and 153 rows), so the filter
  hides nothing from engineers. `account_history` showed 558 versions across the three
  units, the same as the state check.
- **Masks.** As an engineer: account numbers showed as `********4295`, names and emails
  as `REDACTED`, in both silver and the bronze row structs. Account quarantine payloads
  were `REDACTED`, while journal payloads stayed readable. Adding `gl_pii_readers`
  showed every value in clear, and the last four digits matched.
- **No privilege on `bu_filter` is needed.** The analyst queried gold with only the
  Terraform grants. The docs did not say this, and the test settles it.
- **Backing tables are unreadable.** Direct queries on the gold `daily_trial_balance` and
  silver `accounts` backing tables failed with a permission error at every step. That
  confirms what the effective permissions API reported.

Still to do: a full refresh, in checkpoint 5, to see whether `ALTER`-set masks survive it.
(Answered in checkpoint 5: they do. See "Verified on dev" there.)

## Checkpoint 5: Reliability

### Scheduling lives in a job, paused by default

A pipeline has no schedule of its own. `gl_pipeline_scheduled` runs it every 30 minutes
and then runs `trial_balance_check`. Thirty minutes is what the freshness SLA needs: a
file that lands just after an update starts waits for the next one, so its worst case
is 30 minutes plus one update. Hourly could not hold 60 minutes.

Every schedule reads `var.schedule_pause`, which defaults to `PAUSED`. An unpaused prod
runs 48 pipeline updates and 96 freshness checks a day on serverless compute, even with
nothing landing. For a portfolio project that cost is only worth paying during a demo.

### Alerts are job and pipeline emails

Jobs set `email_notifications.on_failure`. The pipeline sets `notifications` for
`on-update-failure`, `on-update-fatal-failure` and `on-flow-failure`. All of them send to
`var.alert_email`, which has no default, so the address is never committed. It comes
from `BUNDLE_VAR_alert_email` locally and from a secret in CI.

### Retries

| Task | Retries | Why |
|---|---|---|
| scheduled pipeline update | 2, a minute apart | failures here are infrastructure; data problems are dropped or quarantined, never raised |
| governance | 2, a minute apart | every statement is idempotent |
| trial_balance_check, freshness_check | 0 | they fail on data; a retry repeats the same answer later |
| backfill_replay | 0 | it copies files and starts updates; a blind retry would re-deliver again |

The Jobs API retries at a fixed interval (`min_retry_interval_millis`), so there is no
true backoff. A minute apart is the closest it gets.

### trial_balance_check tolerates late accounts, for a while

The data quality rules make an orphan journal line warn-only, and the trial balance
check is fail-and-alert. A late account triggers both: its lines sit under a NULL
business_unit, and its real unit is short by the same amount. Failing on that would
alert on every late account, and the alert would soon be ignored.

So a gap counts as explained when both of these hold:

- a NULL-unit group exists for the same currency and entry date, and
- all groups for that currency and date net to zero together.

The second condition alone is not enough. Two real units that are off in opposite
directions also sum to zero, which is what a journal crossing units looks like, and
that has to fail. A unit test covers exactly this case.

An explained gap is tolerated only while every orphan line is younger than
`max_orphan_hours` (48, the generator's maximum lateness). Age comes from `posted_at`,
the source time, not `_ingested_at`, because a full refresh resets ingest time and
would make an old orphan look new. A line whose `posted_at` failed to parse has no age,
so it counts as stale.

### freshness_check measures the oldest file gold has not seen

A landing file is covered once a completed pipeline update that refreshed gold started
after the file landed. The lag is the age of the oldest file not covered. Two details:

- Landing time is the S3 modification time, not the `dt=`/`hh=` partition, which the
  generator names after simulated time.
- Refresh times come from the Pipelines API (`list_updates`), because a materialized
  view has no table history. An update counts if it completed, was not validate-only,
  and either refreshed the whole graph or named `daily_trial_balance` in a selection.

A file that landed at the same instant an update started is treated as not covered.
The update lists files some time after it starts, so the check cannot know whether that
file made it in.

### Date-range replay is a re-delivery

Auto Loader never reads a path twice, so a date range cannot be "reprocessed" in place.
`backfill_replay` in range mode copies the original files for the range into the
current partition as `replay-<job run id>-<name>`. A copy is a new delivery, so it
follows the landing-time partition rule, and it carries events silver has already
applied.

Whether the copies reach AUTO CDC depends on the silver dedupe. It keys on (key, lsn)
with a one hour watermark on `_ingested_at`. The watermark moves with the ingest time
of new data, not with the clock. So a copy is dropped as a duplicate while the stream
still holds state for its original, and reaches AUTO CDC once newer data has pushed the
watermark past it. In prod, where data lands all the time, replaying yesterday is the
second case. The docs do not say what AUTO CDC does with a sequence value it has
already applied, for SCD1 or SCD2. The follow-up test on dev showed that it ignores
them: no row changed and `account_history` gained no versions (see "Verified on dev").

A range may start at most 5 days back (`REPLAY_MAX_AGE_DAYS`). The SCD1 tables keep
delete tombstones for 7 days. A file landed on the first day of the range holds events
up to 48 hours older, and any delete that supersedes one of them came later, so 5 days
keeps every such delete inside the tombstone window. Past that, a re-delivered create
for a deleted key would put the row back. Older ranges need a full refresh, which
replays the deletes too.

`quarantine_events` is an append-only log of deliveries, so a replay can add rows:

- An unparseable line skips the dedupe (it has no key), so every delivery of it becomes
  a row, in a replay and in a full refresh alike.
- A rejected event goes through the dedupe, so it gets a second row only when its copy
  gets past the dedupe, as described above.

Silver and gold must not change either way, but the quarantine row count can.
`silver_state_check` therefore compares quarantine as a set of distinct rows.

### Full refresh goes through the SDK

`pipeline_task` in a job accepts `full_refresh_selection`, but only as a fixed list in
the YAML. The backfill job needs the table list as a run parameter, so its Python task
calls `pipelines.start_update(full_refresh_selection=...)` from the Databricks SDK,
which serverless environment 5 ships (0.67.0). After a selective refresh it runs a
normal update so gold catches up.

Bronze can only be reset together with everything else (`tables=all`). The silver
streams read bronze from a checkpoint, and resetting bronze alone breaks that
checkpoint.

Every backfill run ends with the `governance` job and `governance_check`. This answers
the checkpoint 4 question the safe way: whether or not a full refresh keeps masks set
with `ALTER`, they are back before the job reports success.

### Deferred

The `maintenance` job (OPTIMIZE and VACUUM) moves to checkpoint 6, next to the
compaction experiment that needs its before and after numbers.

### Verified on dev

Run on 2026-09-30 by Anuj, from a cleared landing prefix
(`s3://gl-cdc-lakehouse-anuj/dev/cdc/`), with generator seed 42 throughout.

**Masks survive a full refresh.** After the generator run with
`--withhold-late-accounts` and `gl_pipeline --full-refresh-all`, `governance_check`
passed before the governance job had been run again: 8 classifications, 11 masks with
pii tags, 4 row filters, no `ALL PRIVILEGES`. That answers the checkpoint 4 question.
Masks and filters set with `ALTER` stay on the tables through a full refresh.
`backfill_replay` still reapplies governance after every run, as a guard that costs
little.

**trial_balance_check.**

| Run | State | Result |
|---|---|---|
| 1 | three accounts withheld | warn: 0 unexplained groups, 10 explained by late accounts, 0 stale orphans |
| 2 | same, `max_orphan_hours=0` | fail, failure email received |
| 3 | after `--only-late-accounts` and an update | pass: 0 unexplained, 0 explained, 0 stale |

The 10 explained groups match checkpoint 3's second run: 5 NULL-unit groups and the 5
real unit groups short by the same amounts.

**freshness_check.**

| Run | State | Result |
|---|---|---|
| 1 | a `--minutes 2` generator run, no update, `sla_minutes=1` after 2 minutes | fail, failure email received |
| 2 | after an update | pass: gold refreshed at 15:02:43, newest file 14:57:51, 25 files, all covered |

25 files is every file the three generator runs wrote: 20 from the withhold run, 1
from the late account run and 4 from the two minute run. So the partition listing
found them all.

**Re-runs give identical tables.** `silver_state_check` after each step:

| Step | Result |
|---|---|
| A: before any replay | baseline |
| `backfill_replay mode=range`, today | same as A |
| `backfill_replay mode=refresh tables=all` | same as A |
| `backfill_replay mode=refresh tables=silver.account_history gold.daily_trial_balance` | same as A |

| Table | Rows | Checksum in all four checks |
|---|---|---|
| silver.accounts | 202 | -32674477543491858157 |
| silver.account_history | 494 | -199019209967747125591 |
| silver.journal_entries | 500 | 125848035505143635455 |
| silver.quarantine_events (distinct) | 4 | -12747277404367413034 |
| gold.account_balances | 158 | 60358885540008367057 |
| gold.daily_trial_balance | 32 | 8092906349789054787 |

Every table had 0 missing and 0 extra in all four checks. The selective refresh also
showed that `full_refresh_selection` accepts fully qualified table names.

**What the range replay did and did not prove.** The quarantine reasons after the
replay show what happened to the copies:

| Reason | Before replay | After replay | After refresh all |
|---|---|---|---|
| journal_entries unparseable_payload | 1 | 2 | 2 |
| journal_entries amount_positive | 2 | 2 | 2 |
| journal_entries currency_known | 2 | 2 | 2 |
| accounts currency_known | 1 | 1 | 1 |

The unparseable line, which skips the dedupe, was delivered twice and has two rows.
The rejected events, which go through the dedupe, did not gain rows. So the stream
dedupe dropped the copies. The originals had been ingested less than an hour before the
newest data, so their dedupe state was still held. The parseable copies therefore never
reached AUTO CDC either.

This replay proved that a re-delivery within the dedupe window changes nothing. It did
not prove what AUTO CDC does with a (key, lsn) it has already applied, which is the
prod case: a replay of yesterday arrives long after the dedupe state is gone.

**Follow-up: a replay that reaches AUTO CDC.** Run on 2026-09-30 by Claude, with
Anuj's go-ahead. The full refresh above ran at 16:45 UTC, so every bronze row carried
that ingest time. At 19:33, 2 hours 48 minutes later, two rounds of generator run
(`--minutes 5 --rate 50`) plus pipeline update moved the watermark forward. The second
round is the batch that drops the old dedupe state. Then `silver_state_check` (B),
`backfill_replay mode=range` for 2026-09-30 (45 files: 25 from the earlier runs, 20
from the two new ones), and `silver_state_check` again.

| Table | Rows | Checksum, before and after the replay |
|---|---|---|
| silver.accounts | 204 | -54853975852352771414 |
| silver.account_history | 972 | -179194947190516409550 |
| silver.journal_entries | 938 | 3662668268929214105 |
| silver.quarantine_events (distinct) | 6 | -6573608045740639676 |
| gold.account_balances | 165 | 167976281022534743441 |
| gold.daily_trial_balance | 33 | -51461890706856751947 |

| Reason | Before replay | After replay |
|---|---|---|
| accounts currency_known | 1 | 2 |
| journal_entries amount_positive | 4 | 6 |
| journal_entries currency_known | 4 | 6 |
| journal_entries unparseable_payload | 2 | 3 |

- **The copies got past the dedupe this time.** The rejected reasons rose by exactly
  the rejected events of the 25 older files (1, 2 and 2). The copies of the two new
  runs, ingested minutes before, were still dropped. Valid events leave the same
  deduplicated view as rejected ones, so the valid copies of the older files reached
  AUTO CDC.
- **AUTO CDC ignores a (key, lsn) it has already applied, for SCD1 and SCD2.** Every
  silver and gold checksum is unchanged, and `account_history` has the same 972
  versions. So a replay long after the original delivery leaves silver and gold
  identical, which is the prod case the replay job exists for.
- This rests on one assumption: the AUTO CDC flows had dropped their dedupe state just
  as the quarantine flow had. Each flow keeps its own state, but they read the same
  bronze rows in the same updates, so their watermarks move together.

## Checkpoint 6: Tuning, cost, CI/CD

Experiment numbers are in `docs/results.md`. This section records the decisions.

### CI signs in with GitHub OIDC, as one service principal

The workflows authenticate as the `gl-cicd` service principal through Databricks OAuth
token federation (`DATABRICKS_AUTH_TYPE: github-oidc`). GitHub issues a short-lived
token per job, and Databricks exchanges it because a federation policy on `gl-cicd`
trusts it. So there is no Databricks secret anywhere, in GitHub or elsewhere.

`databricks.yml` names a profile per target (`gl-dev`, `gl-prod`) for laptop use. With a
profile named, CLI 1.8.0 ignores `DATABRICKS_AUTH_TYPE` from the environment, so
environment variables alone fail in CI. Each workflow runs
`.github/scripts/databricks-profiles.sh` first, which writes both profiles into the
runner's `~/.databrickscfg` with `auth_type = github-oidc`. The profiles also set
`audience` from the `DATABRICKS_TOKEN_AUDIENCE` repository variable, which must equal
the audience in the federation policies: the `gl-cicd` application ID. The audience is
only a value the token and the policy must agree on. The Databricks docs suggest the
account ID; the application ID works the same, and ties tokens to this one service
principal's policies. Without it,
the CLI asks GitHub for a token whose audience is the workspace token endpoint (read
from the SDK source, `determineAudience`), and the exchange would be refused.

The policies trust GitHub environments, not branches:

- `repo:anjmhrjn@57608084/gl-cdc-lakehouse@1396470167:environment:dev` for PR
  validation and the deploy on merge to main
- `repo:anjmhrjn@57608084/gl-cdc-lakehouse@1396470167:environment:prod` for the deploy
  on a `v*` tag

GitHub puts the numeric owner and repository IDs next to the names in the token
subject for this repository, so the subject is not the `repo:<owner>/<repo>:...` form
in the Databricks example. The IDs tie the policy to this repository: a repository
created later under the same name would have a different ID and would not match. The
subject a job actually sends is printed in the error when the exchange is refused.

The prod environment in GitHub only accepts `v*` tags, so no branch or pull request can
get a token through the prod policy. A branch subject (`ref:refs/heads/main`) would not
match pull requests or tags, which have their own subjects.

One service principal serves both environments, and it is in `gl_engineers`, so a dev
token could in principle deploy prod. That is accepted for a one-person repo where only
the owner can push branches. On a team there would be one service principal per
environment, each granted only its own catalog.

### Dev is one shared deployment

A table belongs to exactly one pipeline. If CI deployed dev as `gl-cicd` into its own
deployment while Anuj deployed from a laptop into his, there would be two dev pipelines
trying to publish the same `gl_dev` tables. So both deploy the same deployment:

- The bundle state lives in `gl-cicd`'s home folder, set by `workspace.root_path`, not
  in each deployer's home.
- `gl-cicd` owns every job and the pipeline (`IS_OWNER`), and everything runs as
  `gl-cicd` (`run_as`), as in prod. `gl_engineers` has `CAN_MANAGE`.

Ownership has to be named. Without an `IS_OWNER` in the config, the CLI makes whoever
deploys the owner (`FixPermissions` in the CLI source). The first CI deploy then tried
to take ownership of jobs Anuj owned, and only a workspace admin may change a job's
owner, so it failed with 403. With `gl-cicd` named as owner, the deployer gets
`CAN_MANAGE` instead and nothing changes hands. `IS_OWNER` is only accepted on each
resource, not at target level, so the dev target lists every job and the pipeline,
through one YAML anchor. A new job must be added to that list, or CI's deploy fails on
it.

A laptop deploy needs the Service Principal User role on `gl-cicd`, because it binds
`gl-cicd` as `run_as`. Anuj holds it; it is not granted to `gl_engineers`, since anyone
with it can run code as `gl-cicd`, which reads unmasked PII. CI deploys as `gl-cicd`
itself and needs no role.

Moving dev from Anuj to `gl-cicd` (2026-10-01):

- Jobs: a laptop deploy by Anuj, as workspace admin, changed their owner.
- Pipeline: only a metastore admin can change a pipeline's owner, and no person here
  is one. The dev pipeline was deleted instead, which dropped its tables, and the
  next CI deploy created it again as `gl-cicd`. The tables were rebuilt from the
  landing files, which the deletion does not touch.
- Governance functions: transferred to `gl_engineers` first. A non-admin owner can only
  hand a function to a group they belong to, and `gl-cicd` is a member, so it could
  replace them. The governance job's `CREATE OR REPLACE FUNCTION` then recreated them
  as `gl-cicd`, which now owns them, as it will in prod.

After the move, a full refresh, `governance`, `governance_check` and
`silver_state_check` all ran as `gl-cicd` and passed, with every checksum equal to the
checkpoint 6 runs in `docs/results.md`.

`mode: development` refuses a root path outside the deployer's own folder. So dev sets
the parts of development mode it needs as presets: `name_prefix: "[dev] "` and
`pipelines_development: true`. Schedules are paused by `schedule_pause`. Development
mode would also have turned off the deployment lock, and the shared deployment needs
it on, so that a CI deploy and a laptop deploy cannot overlap.

`/Workspace/Shared` was rejected for the state: every workspace user can write there,
and so could change the code a dev job runs as its owner.

Moving the state into the shared folder kept every resource: the first deploy with the
new root path used the local state, so the pipeline and all jobs were updated in place
with their IDs unchanged.

### Prod runs as gl-cicd

Prod is deployed only by CI, on a tag, and `run_as` makes every job and the pipeline run
as `gl-cicd`. Prod then depends on no person's account. Because the pipeline runs as
`gl-cicd`, it had to join `gl_pii_readers` as well as `gl_engineers` (see "The pipeline
owner must be in gl_pii_readers and gl_engineers"). In prod `gl-cicd` creates the
pipeline, so it owns it from the start and no ownership change is ever needed. The
governance functions in `gl_prod` will be owned by `gl-cicd`, since it is the first to
run the governance job there.

### Predictive optimization does routine maintenance

The metastore has predictive optimization enabled, and `gl_dev` inherits it. The
pipeline's streaming tables carry
`spark.internal.streaming_table.predictive_optimization.enabled = true`. So OPTIMIZE and
VACUUM already run in the background, and the `maintenance` job has no schedule. It
runs OPTIMIZE (and VACUUM with the default 7 day retention) on demand, after a large
backfill or for an experiment. OPTIMIZE on a pipeline's streaming table from a job was
confirmed to work on dev. The gold materialized views are left out: every update
recomputes them, and DESCRIBE DETAIL does not accept them.

### System tables are read by people, not by jobs

Billing and query history (`system.billing`, `system.query`) are readable by Anuj only.
Every dev and prod job runs as `gl-cicd`, which is not granted them: billing covers the
whole account, and nothing the CI identity runs needs it. The two system table queries
the experiments used are SQL files in `sql/analysis/`, run by hand in the SQL editor or
a notebook. The `tuning_probe` job keeps only what `gl-cicd` can read: table details,
the pipeline's event log (it owns the pipeline), and the benchmark and skew queries.

### Two experiment settings stay in the bundle

`max_files_per_trigger` (Auto Loader files per micro-batch, default 1000) and
`journal_target_file_size` (default `auto`) exist to reproduce the experiments in
`docs/results.md`. At their defaults they change nothing: 1000 is Auto Loader's own
default, and `auto` sets no table property.

### SCD2 history, as AUTO CDC writes it

The medium dataset showed two AUTO CDC behaviors that the small runs had not:

- An update whose values equal the open version's opens no new version. The docs say
  SCD type 2 creates a version "whenever any column value changes".
- A delete right after another delete for the same key has no open version to close.
  AUTO CDC then writes a closed row from the delete's before image, with a NULL
  `__START_AT` and the delete's lsn as `__END_AT`. This is observed, not documented.

The generator produces the second case because it can pick an already deleted account
to delete. A real source would not, but it is kept as one more kind of messy input.
Gold is not affected: `latest_accounts` orders versions by `__START_AT DESC`, which puts
the NULL row last.

### Prod root path names gl-cicd's home

Prod's `root_path` was `/Workspace/Users/${workspace.current_user.userName}/...`. CI
deploys as `gl-cicd`, so the state landed in its home, but a laptop resolved the same
setting to the laptop user's home, found no state, and `bundle run -t prod` could not
find the deployed jobs. The path now names `gl-cicd`'s home directly. For CI that is
the same path as before, so the deployment did not move.

### Verified on prod

Run on 2026-10-01. Anuj applied `envs/prod.tfvars` (12 added), wrote a small prod batch
(`--env prod --minutes 10 --rate 50`) and pushed tag `v0.1.0`; `deploy-prod` passed. Then,
as `gl-cicd`:

| Step | Result |
|---|---|
| first `gl_pipeline_prod` update | completed, no errors or warnings |
| `governance` | applied |
| `governance_check` | 8 classifications, 11 masks with pii tags, 4 row filters, no `ALL PRIVILEGES` |
| `trial_balance_check` | pass: 0 unexplained groups, 0 stale orphans |
| `silver_state_check` | every table matches, 0 missing, 0 extra |

| Table | Rows |
|---|---|
| silver.accounts | 203 |
| silver.account_history | 279 |
| silver.journal_entries | 408 |
| silver.quarantine_events | 4 |
| gold.account_balances | 145 |
| gold.daily_trial_balance | 32 |

So the same commit runs in dev and prod with only `env` changed, through CI, with no
person's account involved in prod. Schedules stay paused; unpausing is a RUNBOOK step.
