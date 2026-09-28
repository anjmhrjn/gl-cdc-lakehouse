provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile

  default_tags {
    tags = {
      project   = "gl-cdc-lakehouse"
      managedby = "terraform"
    }
  }
}

# Workspace-level Databricks provider. Everything here (storage credential, external
# location, catalog, schema, grants) is a workspace-level Unity Catalog API. The
# metastore, workspace and account groups are created manually and stay out of state.
provider "databricks" {
  profile = var.databricks_profile
}

data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}
