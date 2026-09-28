variable "aws_region" {
  type        = string
  default     = "us-east-2"
  description = "Must match the Unity Catalog metastore region."
}

variable "aws_profile" {
  type    = string
  default = "gl-lakehouse"
}

variable "databricks_profile" {
  type    = string
  default = "gl-dev"
}

variable "prefix" {
  type    = string
  default = "gl-cdc-lakehouse"
}

variable "landing_bucket" {
  type        = string
  default     = "gl-cdc-lakehouse-anuj"
  description = "One bucket for all environments. Each env gets its own top-level prefix."
}

variable "environments" {
  type        = list(string)
  description = <<-EOT
    Environments to materialise. dev and prod share one bucket, one KMS key, one IAM
    role and one storage credential, so they also share one state file. The env list
    is what varies between var-files: envs/dev.tfvars builds dev only, envs/prod.tfvars
    builds both. Per env this creates an external location, a catalog gl_<env>, the
    bronze/silver/gold schemas and their grants.
  EOT
}

variable "engineer_group" {
  type    = string
  default = "gl_engineers"
}

variable "analyst_groups" {
  type = map(string)
  default = {
    TREASURY     = "gl_analysts_treasury"
    CORP_BANKING = "gl_analysts_corp_banking"
    ASSET_MGMT   = "gl_analysts_asset_mgmt"
  }
}
