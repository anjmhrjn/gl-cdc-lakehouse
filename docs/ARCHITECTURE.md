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

### Late arrivals get a genuinely older lsn

The run's `lsn` counter starts at 5,000,000. Late events draw from the band below it,
scaled by how far back they are dated, up to `--late-hours` (48 by default). So a late
event is not just old by timestamp, it is old by the sequence key that silver actually
orders on. Three accounts are also held back so their journal entries land before the
account exists, which exercises the warn-only orphan foreign key rule.

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
