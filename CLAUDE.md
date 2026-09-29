# GL CDC Lakehouse

## What this is

A portfolio project: a change data capture (CDC) pipeline for general ledger data on Databricks on AWS.

- Synthetic Debezium-format change events land in S3.
- They stream through Lakeflow Declarative Pipelines (formerly Delta Live Tables) into bronze, silver, and gold Delta tables.
- Unity Catalog governs the tables.
- Terraform manages the infrastructure; Databricks Asset Bundles manage pipelines and jobs.

The goal is to show production data engineering practice: CDC, streaming plus batch, idempotent reprocessing, PII governance, IaC, testing, tuning, and cost awareness. Correctness and explainability matter more than feature count. Every decisions should be explainable.

## Architecture

```
generator (local Python CLI)
  -> s3://gl-cdc-lakehouse-anuj/cdc/{accounts,journal_entries}/   JSON, SSE-KMS
  -> bronze: Auto Loader, append-only, raw payload + ingest metadata
  -> silver: AUTO CDC (APPLY CHANGES) keyed on PK, sequenced by source.lsn
       accounts (SCD1), account_history (SCD2), journal_entries (SCD1, deletes applied)
       expectations + quarantine table
  -> gold: account_balances, daily_trial_balance (materialized views)

Jobs: backfill_replay, freshness_check, trial_balance_check, maintenance (OPTIMIZE/VACUUM), governance (apply SQL)
```

## Repo layout

```
infra/terraform/          AWS + Unity Catalog resources; envs/dev.tfvars, envs/prod.tfvars
generator/                CDC event generator (Python CLI)
src/pipelines/            pipeline definitions: bronze.py, silver.py, gold.py
src/transforms/           pure PySpark functions used by pipelines; unit tested
src/jobs/                 backfill_replay.py, freshness_check.py, trial_balance_check.py, maintenance.py
sql/governance/           tags, mask functions, row filters, grants (idempotent)
resources/                bundle resource YAML (pipeline, jobs)
tests/unit/               local PySpark tests
tests/integration/        run against gl_dev after deploy
docs/                     ARCHITECTURE.md, RUNBOOK.md, results.md, AI_USAGE.md
databricks.yml
.github/workflows/
```

## Data contract: CDC event

One JSON object per event:

```json
{
  "op": "c | u | d | r",
  "ts_ms": 1727450000000,
  "source": {"table": "accounts", "lsn": 1048576, "tx_id": 812},
  "before": {"...": "row before change, null for c/r"},
  "after":  {"...": "row after change, null for d"}
}
```

- `source.lsn` is the monotonic sequence key. Always sequence by `lsn`, never by `ts_ms` or arrival time.
- For `op = d`, `after` is null and the key comes from `before`.
- `op = r` is a snapshot or backfill read. Treat it as an upsert.
- The generator deliberately emits:
  - exact duplicates (~2%)
  - out-of-order events within a file
  - late arrivals: events whose lsn is older than events already processed, delayed up to 48h
  - malformed records (~0.5%)
  - one hot account holding ~30% of journal entries, for skew testing

### accounts

| column | type | notes |
|---|---|---|
| account_id | STRING | PK |
| account_number | STRING | PII |
| holder_name | STRING | PII |
| holder_email | STRING | PII |
| business_unit | STRING | TREASURY, CORP_BANKING, ASSET_MGMT |
| account_type | STRING | ASSET, LIABILITY, EQUITY, REVENUE, EXPENSE |
| currency | STRING | ISO 4217 |
| status | STRING | OPEN, FROZEN, CLOSED |
| opened_at | TIMESTAMP | |
| updated_at | TIMESTAMP | |

### journal_entries

| column | type | notes |
|---|---|---|
| entry_id | STRING | PK |
| journal_id | STRING | groups the lines of one balanced journal |
| account_id | STRING | FK to accounts |
| entry_date | DATE | |
| amount | DECIMAL(18,2) | > 0 |
| side | STRING | DEBIT, CREDIT |
| currency | STRING | ISO 4217 |
| description | STRING | |
| posted_at | TIMESTAMP | |
| updated_at | TIMESTAMP | |

Invariant: for each journal_id, total debits equal total credits.

## Tables

Each environment has its own catalog: `gl_dev`, `gl_prod`.

- **bronze:**
  - `accounts_cdc_raw`, `journal_entries_cdc_raw`
  - Append-only.
  - Keep `_ingested_at`, `_source_file`, `_rescued_data`.
- **silver:**
  - `accounts` (SCD1), `account_history` (SCD2), `journal_entries` (SCD1, deletes applied)
  - `quarantine_events`
- **gold:**
  - `account_balances`, `daily_trial_balance`

## Data quality rules

- **Drop:** missing PK, null `lsn`, unknown `op`.
- **Quarantine** (write to `silver.quarantine_events`, do not fail the pipeline): `amount <= 0`, unknown currency, unparseable payload.
- **Warn only:** journal entry whose `account_id` has no account yet. This is usually a late-arriving account, so keep the row and reconcile when the account lands.
- **Fail and alert:** `trial_balance_check` job, if debits and credits do not net to zero per business_unit, currency, and entry_date.

## Governance

- **Column tags:**
  - `pii = true` on account_number, holder_name, holder_email.
  - `classification = confidential | internal` on all silver and gold tables.
- **Column masks:**
  - Mask account_number (show last 4), holder_name, and holder_email for anyone not in `gl_pii_readers`.
- **Row filter** on business_unit:
  - `gl_analysts_treasury`, `gl_analysts_corp_banking`, and `gl_analysts_asset_mgmt` each see only their unit.
  - `gl_engineers` see all rows.
- **Least privilege:**
  - Analysts get SELECT on gold only.
  - Engineers get access to all schemas.
  - No human gets ALL PRIVILEGES on a catalog.
- **Where it runs:** governance SQL lives in `sql/governance/`, must be idempotent (CREATE OR REPLACE, IF NOT EXISTS), and runs as a bundle job.
- **Groups:** account-level groups are created manually, not in code.

## Terraform scope

**In scope:**
- S3 landing bucket: versioning, SSE-KMS, public access blocked.
- KMS key and key policy.
- IAM role for the Unity Catalog storage credential, following the trust policy in current Databricks docs.
- Unity Catalog storage credential, external location, catalogs, schemas, and catalog/schema grants.

**Out of scope:**
- Workspace, VPC, metastore, and account groups (manual).
- Pipelines and jobs, which belong to the Asset Bundle.

**State:** local and gitignored. This is a solo project; the README explains how remote state would work on a team.

## Environments

- Two bundle targets, `dev` and `prod`, in one workspace.
- Every catalog, path, and schedule derives from a single `env` variable.
- Dev: the pipeline runs in triggered mode with development mode on.
- Prod: runs on a schedule.
- Never make any pipeline continuous.

## Reliability requirements

- **Idempotent re-runs:** re-running any pipeline or job over the same input must produce identical tables. Integration tests verify this with row counts and checksums.
- **Backfill/replay:** the `backfill_replay` job reprocesses a date range of landing files or full-refreshes selected tables. RUNBOOK.md documents it step by step.
- **Retries:** up to 2 with backoff for transient errors. Never retry on data quality failures.
- **Freshness SLA:** `gold.daily_trial_balance` updates within 60 minutes of the latest landed file. Otherwise `freshness_check` fails and sends an alert.

## Performance and cost

- **Log every tuning experiment** in `docs/results.md`: what changed, before and after numbers (file count, duration, bytes scanned), and why it worked.
- **Planned experiments:**
  - small-file compaction
  - liquid clustering on journal_entries (account_id, entry_date)
  - fixing the skewed join on the hot account
- **Cost:**
  - Record cost per pipeline run from `system.billing.usage` in `docs/results.md`.
  - Compute is serverless only (serverless workspace). No classic clusters.
  - No always-on warehouses.

## Testing

- **Unit tests:** transforms in `src/transforms/` are pure functions over DataFrames. Test them with local PySpark, pytest, and chispa.
- **Tests before implementation** for:
  - CDC sequencing: out-of-order events, duplicates, deletes, late arrivals
  - SCD2 history correctness
  - the journal balance invariant
  - masking output
- **Integration tests** run against `gl_dev` after deploy.
- **CI:**
  - Every PR: ruff, unit tests, `databricks bundle validate`.
  - Merge to main: deploy to dev.
  - Git tag: deploy to prod.

## Working rules for Claude

- **One checkpoint at a time.** Start each task in plan mode, list the files you will change, and wait for approval.
- **No approval, no billable changes.** Never run `terraform apply`, `terraform destroy`, `databricks bundle deploy -t prod`, or anything that starts billable compute without explicit approval in the current session. `terraform plan` and `databricks bundle validate` are fine.
- **Never touch credentials.**
  - Don't read, print, or write them.
  - Auth comes from AWS profile `gl-lakehouse` and Databricks profiles `gl-dev` and `gl-prod`.
  - Nothing secret goes in code, tfvars, or commits.
- **Verify Databricks APIs before using them.** Names change often: DLT became Lakeflow Declarative Pipelines, and APPLY CHANGES became AUTO CDC.
  - Check current docs before using an API.
  - Use one naming style consistently.
  - Never guess parameters. If unsure, say so.
- **Explain non-obvious choices** in a short code comment or in `docs/ARCHITECTURE.md`.
- **Log your mistakes.** When Anuj corrects your output or a test catches your mistake, add an entry to `docs/AI_USAGE.md` with:
  - what you produced
  - what was wrong
  - how it was caught
  - the fix
- **Log Terraform actions.** Every `terraform` command that changes state or reveals something (`init`, `plan`, `apply`, `import`, `state` operations) gets an entry at the top of `notes/terraform-log.md`: the command, who ran it, why, the result, and what it teaches. Also log manual infrastructure steps that Terraform could not do. The file is gitignored local learning notes, not project documentation.
- **Keep it simple.** No abstractions, frameworks, or tools beyond what this file scopes.
- **Docs style:** plain, direct language. No marketing words. No em dashes.

## Commands

```
uv run pytest tests/unit
uv run ruff check .
uv run python -m generator --env dev --minutes 10 --rate 50
terraform -chdir=infra/terraform plan -var-file=envs/dev.tfvars
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run gl_pipeline -t dev
```

## Checkpoints

Each day is done only when its tests pass and the listed docs are updated.

1. **Terraform and generator.** `terraform plan` is clean for dev, and apply succeeds (run by Anuj). The generator writes valid, messy CDC files to S3.
2. **Bronze and silver CDC.** Unit tests for sequencing and SCD2 pass. Silver tables match the expected state after a generator run.
3. **Quality and gold.** Expectations and quarantine work, gold tables build, and late-arriving events reconcile correctly.
4. **Governance.** Tags, masks, row filters, and grants are applied and verified by toggling group membership. Masking tests pass.
5. **Reliability.**
   - Backfill/replay works, with re-runs proven identical.
   - Retries, the freshness alert, and the trial balance check are in place.
   - RUNBOOK.md is written.
6. **Tuning, cost, and CI/CD.** Experiments and the cost query are logged in results.md. CI runs, and dev to prod promotion works.
7. **Docs.** The README includes an architecture diagram and results table. ARCHITECTURE.md and AI_USAGE.md are complete.