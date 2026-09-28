locals {
  uc_role_name = "${var.prefix}-uc-access"
  # Built as a string, not a reference to aws_iam_role.uc. The storage credential has
  # to exist before the role's trust policy can be written (the credential generates
  # the external id the trust policy needs), so referencing the role here would make
  # a cycle. See unity_catalog.tf.
  uc_role_arn = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:role/${local.uc_role_name}"
}

resource "aws_kms_key" "landing" {
  description             = "SSE-KMS for the GL CDC landing bucket"
  enable_key_rotation     = true
  deletion_window_in_days = 7
  policy                  = data.aws_iam_policy_document.kms.json
}

resource "aws_kms_alias" "landing" {
  name          = "alias/${var.prefix}"
  target_key_id = aws_kms_key.landing.key_id
}

data "aws_iam_policy_document" "kms" {
  # Without this the key becomes unmanageable: only the key policy grants access to
  # a KMS key, IAM policies alone are not enough.
  statement {
    sid     = "AccountRootAdmin"
    effect  = "Allow"
    actions = ["kms:*"]
    principals {
      type        = "AWS"
      identifiers = ["arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root"]
    }
    resources = ["*"]
  }

  statement {
    sid    = "UnityCatalogRoleUse"
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GenerateDataKey*",
      "kms:DescribeKey",
    ]
    principals {
      type        = "AWS"
      identifiers = [local.uc_role_arn]
    }
    resources = ["*"]
  }

  statement {
    sid    = "GeneratorUse"
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GenerateDataKey*",
      "kms:DescribeKey",
    ]
    principals {
      type        = "AWS"
      identifiers = [data.aws_caller_identity.current.arn]
    }
    resources = ["*"]
  }
}
