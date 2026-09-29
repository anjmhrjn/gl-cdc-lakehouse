# GL CDC Lakehouse

A change data capture pipeline for general ledger data on Databricks on AWS.

Synthetic Debezium-format change events land in S3, stream through Lakeflow Declarative
Pipelines into bronze, silver and gold Delta tables, and are governed by Unity Catalog.
Terraform manages the AWS and Unity Catalog resources. Databricks Asset Bundles manage
the pipeline and jobs.

See `docs/ARCHITECTURE.md` for the design and `CLAUDE.md` for the full scope.

## Layout

```
infra/terraform/   S3, KMS, IAM, Unity Catalog resources
generator/         CDC event generator (Python CLI)
src/               pipelines, transforms, jobs
sql/governance/    tags, masks, row filters, grants
resources/         bundle resource YAML
tests/             unit and integration tests
docs/              architecture, runbook, results, AI usage
```

## Setup

Requires the AWS profile `gl-lakehouse` and the Databricks profiles `gl-dev` and `gl-prod`.

```
uv sync
uv run ruff check .
uv run pytest tests/unit
```

## Infrastructure

The `gl-lakehouse` IAM user needs permission to create the S3 bucket, the KMS key and
alias, and IAM roles named `gl-cdc-lakehouse-*`, before the first apply. Terraform cannot
grant that, because the permissions are for the identity running Terraform, so it is
attached once by hand as an account admin.

```
terraform -chdir=infra/terraform init
terraform -chdir=infra/terraform plan  -var-file=envs/dev.tfvars
terraform -chdir=infra/terraform apply -var-file=envs/dev.tfvars
```

Dev and prod share one landing bucket, one KMS key, one IAM role and one storage
credential, and therefore one state file. `envs/prod.tfvars` is a superset of
`envs/dev.tfvars`: applying it adds the prod external location, catalog and schemas
without touching dev.

### State

State is local and gitignored, which is fine for one person on one machine. On a team
it would move to an S3 backend with versioning on and a DynamoDB table for locking:

```hcl
terraform {
  backend "s3" {
    bucket         = "gl-cdc-lakehouse-tfstate"
    key            = "lakehouse/terraform.tfstate"
    region         = "us-east-2"
    dynamodb_table = "gl-cdc-lakehouse-tflock"
    encrypt        = true
  }
}
```

That gives shared state, locking so two applies cannot race, and a version history to
roll back to. The bucket and table would be bootstrapped once by hand or in a separate
config, because a backend cannot create its own storage.

## Generating events

```
uv run python -m generator --env dev --minutes 10 --rate 50
uv run python -m generator --env dev --minutes 2 --rate 20 --out-dir /tmp/gl-sample
```

`--out-dir` writes locally instead of S3. The same `--seed` always produces the same
events, which is what makes the idempotent re-run tests meaningful.
