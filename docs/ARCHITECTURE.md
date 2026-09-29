# Architecture

This file records decisions that are not obvious from reading the code. The overall
shape of the project is in `CLAUDE.md`.

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

Files are keyed `cdc/<table>/dt=YYYY-MM-DD/hh=HH/part-<epoch>-<seq>.json`. A late
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
