resource "aws_iam_role" "uc" {
  name = local.uc_role_name
  # Trust policy carries the external id Databricks generated for the storage
  # credential, plus the self-assume statement AWS has required since June 2023 and
  # Databricks has enforced since January 2025. The data source emits both.
  assume_role_policy = data.databricks_aws_unity_catalog_assume_role_policy.this.json
}

data "aws_iam_policy_document" "uc" {
  statement {
    sid    = "LandingBucketAccess"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
      "s3:ListBucketMultipartUploads",
      "s3:ListMultipartUploadParts",
      "s3:AbortMultipartUpload",
    ]
    resources = [
      aws_s3_bucket.landing.arn,
      "${aws_s3_bucket.landing.arn}/*",
    ]
  }

  statement {
    sid    = "LandingKeyAccess"
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GenerateDataKey*",
    ]
    resources = [aws_kms_key.landing.arn]
  }

  statement {
    sid       = "SelfAssume"
    effect    = "Allow"
    actions   = ["sts:AssumeRole"]
    resources = [local.uc_role_arn]
  }
}

resource "aws_iam_role_policy" "uc" {
  name   = "${var.prefix}-uc-access"
  role   = aws_iam_role.uc.id
  policy = data.aws_iam_policy_document.uc.json
}

# IAM is eventually consistent. Without a pause the external location's validation
# call can fire before the role is assumable and fail the first apply.
resource "time_sleep" "iam_propagation" {
  depends_on      = [aws_iam_role.uc, aws_iam_role_policy.uc]
  create_duration = "30s"
}
