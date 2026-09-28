# Superset on purpose. dev and prod share the bucket, KMS key, IAM role and storage
# credential, so they share one state file. Applying this var-file adds the prod
# external location, catalog and schemas without touching anything dev owns.
databricks_profile = "gl-prod"
environments       = ["dev", "prod"]
