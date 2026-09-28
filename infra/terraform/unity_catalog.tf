locals {
  schemas = ["bronze", "silver", "gold"]

  env_schemas = {
    for pair in setproduct(var.environments, local.schemas) :
    "${pair[0]}_${pair[1]}" => { env = pair[0], schema = pair[1] }
  }
}

# Created before the IAM role exists, because the role's trust policy needs the
# external id this resource generates. Validation is skipped for that reason; the
# external location below does validate, and it runs after the role is in place.
resource "databricks_storage_credential" "landing" {
  name            = replace("${var.prefix}-landing", "-", "_")
  comment         = "Access to the GL CDC landing bucket"
  skip_validation = true

  aws_iam_role {
    role_arn = local.uc_role_arn
  }
}

data "databricks_aws_unity_catalog_assume_role_policy" "this" {
  aws_account_id = data.aws_caller_identity.current.account_id
  role_name      = local.uc_role_name
  external_id    = databricks_storage_credential.landing.aws_iam_role[0].external_id
}

resource "databricks_grants" "landing_credential" {
  storage_credential = databricks_storage_credential.landing.id

  grant {
    principal  = var.engineer_group
    privileges = ["CREATE_EXTERNAL_TABLE", "READ_FILES", "WRITE_FILES"]
  }
}

resource "databricks_external_location" "landing" {
  for_each = toset(var.environments)

  name            = "gl_${each.key}_landing"
  url             = "s3://${aws_s3_bucket.landing.id}/${each.key}/cdc"
  credential_name = databricks_storage_credential.landing.id
  comment         = "CDC landing files for ${each.key}"

  depends_on = [
    time_sleep.iam_propagation,
    aws_s3_bucket_policy.landing,
  ]
}

resource "databricks_grants" "landing_location" {
  for_each = databricks_external_location.landing

  external_location = each.value.id

  grant {
    principal  = var.engineer_group
    privileges = ["CREATE_EXTERNAL_TABLE", "READ_FILES", "WRITE_FILES"]
  }
}

# Managed table storage. This account has Default Storage enabled and the metastore has
# no storage root, so every catalog has to name its own managed location, and that
# location has to sit inside an external location. Kept as a sibling prefix of the
# landing files: Unity Catalog refuses a managed location that overlaps a path holding
# external tables, and mixing the two would make lifecycle rules on landing files
# dangerous.
resource "databricks_external_location" "managed" {
  for_each = toset(var.environments)

  name            = "gl_${each.key}_managed"
  url             = "s3://${aws_s3_bucket.landing.id}/${each.key}/managed"
  credential_name = databricks_storage_credential.landing.id
  comment         = "Managed table storage for gl_${each.key}"

  depends_on = [
    time_sleep.iam_propagation,
    aws_s3_bucket_policy.landing,
  ]
}

# Only CREATE_MANAGED_STORAGE. Managed table files are reached through the tables, never
# read as files, so READ_FILES and WRITE_FILES would be more than anyone needs here.
resource "databricks_grants" "managed_location" {
  for_each = databricks_external_location.managed

  external_location = each.value.id

  grant {
    principal  = var.engineer_group
    privileges = ["CREATE_MANAGED_STORAGE"]
  }
}

resource "databricks_catalog" "env" {
  for_each = toset(var.environments)

  name         = "gl_${each.key}"
  comment      = "General ledger lakehouse, ${each.key}"
  owner        = var.engineer_group
  storage_root = databricks_external_location.managed[each.key].url

  properties = {
    environment = each.key
  }

  depends_on = [databricks_grants.managed_location]
}

resource "databricks_schema" "env" {
  for_each = local.env_schemas

  catalog_name = databricks_catalog.env[each.value.env].name
  name         = each.value.schema
  owner        = var.engineer_group
  comment      = "${each.value.schema} layer"
}

# Analysts can traverse the catalog but get no schema privileges here. Their only
# data access is SELECT on gold, granted below. Nobody gets ALL PRIVILEGES.
resource "databricks_grants" "catalog" {
  for_each = databricks_catalog.env

  catalog = each.value.name

  grant {
    principal  = var.engineer_group
    privileges = ["USE_CATALOG"]
  }

  dynamic "grant" {
    for_each = var.analyst_groups
    content {
      principal  = grant.value
      privileges = ["USE_CATALOG"]
    }
  }
}

resource "databricks_grants" "schema" {
  for_each = databricks_schema.env

  schema = each.value.id

  grant {
    principal = var.engineer_group
    privileges = [
      "USE_SCHEMA",
      "CREATE_TABLE",
      "CREATE_MATERIALIZED_VIEW",
      "CREATE_FUNCTION",
      "MODIFY",
      "SELECT",
      "REFRESH",
      "APPLY_TAG",
    ]
  }

  dynamic "grant" {
    for_each = local.env_schemas[each.key].schema == "gold" ? var.analyst_groups : {}
    content {
      principal  = grant.value
      privileges = ["USE_SCHEMA", "SELECT"]
    }
  }
}
