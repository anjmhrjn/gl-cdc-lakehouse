output "landing_bucket" {
  value = aws_s3_bucket.landing.id
}

output "kms_key_arn" {
  value = aws_kms_key.landing.arn
}

output "uc_role_arn" {
  value = aws_iam_role.uc.arn
}

output "storage_credential_external_id" {
  description = "Set in the IAM role trust policy. Useful when debugging access denials."
  value       = databricks_storage_credential.landing.aws_iam_role[0].external_id
}

output "external_locations" {
  value = { for k, v in databricks_external_location.landing : k => v.url }
}

output "catalogs" {
  value = { for k, v in databricks_catalog.env : k => v.name }
}

output "managed_locations" {
  value = { for k, v in databricks_external_location.managed : k => v.url }
}
