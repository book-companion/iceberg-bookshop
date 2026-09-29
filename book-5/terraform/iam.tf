# Four least-privilege roles for one table bucket: read, write, maintenance
# and admin. Action names are taken from the Service Authorization Reference
# for Amazon S3 Tables (see ../README.md); none of these policies has been
# tested against IAM, so treat a denial as the policy's fault until shown
# otherwise.
#
# Scope: every statement names this bucket or its tables, never "*", except
# ListTableBuckets, which has no resource to scope to.

locals {
  bucket_arn = aws_s3tables_table_bucket.bookshop.arn
  tables_arn = "${aws_s3tables_table_bucket.bookshop.arn}/table/*"

  # Discovery: enough to find the bucket, the namespace and the tables.
  discover_actions = [
    "s3tables:GetTableBucket",
    "s3tables:GetNamespace",
    "s3tables:ListNamespaces",
    "s3tables:ListTables",
  ]

  # Reading a table: its metadata pointer and its data files.
  read_table_actions = [
    "s3tables:GetTable",
    "s3tables:GetTableMetadataLocation",
    "s3tables:GetTableData",
  ]

  # Committing to an existing table: write files, then swap the pointer.
  write_table_actions = [
    "s3tables:PutTableData",
    "s3tables:UpdateTableMetadataLocation",
  ]

  maintenance_bucket_actions = [
    "s3tables:GetTableBucketMaintenanceConfiguration",
    "s3tables:PutTableBucketMaintenanceConfiguration",
  ]

  maintenance_table_actions = [
    "s3tables:GetTable",
    "s3tables:GetTableMaintenanceConfiguration",
    "s3tables:PutTableMaintenanceConfiguration",
    "s3tables:GetTableMaintenanceJobStatus",
    # Required to set the sort or z-order compaction strategy explicitly
    # (S3 User Guide, s3-tables-maintenance.html). It also lets this role
    # read table data, which is the price of that documented requirement.
    "s3tables:GetTableData",
  ]

  admin_bucket_actions = [
    "s3tables:CreateNamespace",
    "s3tables:DeleteNamespace",
    "s3tables:CreateTable",
    "s3tables:GetTableBucketPolicy",
    "s3tables:PutTableBucketPolicy",
    "s3tables:DeleteTableBucketPolicy",
    "s3tables:GetTableBucketEncryption",
    "s3tables:PutTableBucketEncryption",
  ]

  admin_table_actions = [
    "s3tables:DeleteTable",
    "s3tables:RenameTable",
    "s3tables:GetTablePolicy",
    "s3tables:PutTablePolicy",
    "s3tables:DeleteTablePolicy",
    "s3tables:GetTableEncryption",
    "s3tables:PutTableEncryption", # permission-only: CreateTable with an encryption setting
    "s3tables:TagResource",
    "s3tables:UntagResource",
    "s3tables:ListTagsForResource",
  ]
}

# ---------------------------------------------------------------- trust

data "aws_iam_policy_document" "trust" {
  for_each = var.trusted_principal_arns

  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = each.value
    }
  }
}

# ---------------------------------------------------------------- reader

data "aws_iam_policy_document" "reader" {
  statement {
    sid       = "ListBuckets"
    actions   = ["s3tables:ListTableBuckets"]
    resources = ["*"]
  }
  statement {
    sid       = "Discover"
    actions   = local.discover_actions
    resources = [local.bucket_arn]
  }
  statement {
    sid       = "ReadTables"
    actions   = local.read_table_actions
    resources = [local.tables_arn]
  }

  dynamic "statement" {
    for_each = var.kms_key_arn == null ? [] : [var.kms_key_arn]
    content {
      sid       = "DecryptWithTableKey"
      actions   = ["kms:Decrypt"]
      resources = [statement.value]
    }
  }
}

# ---------------------------------------------------------------- writer

data "aws_iam_policy_document" "writer" {
  source_policy_documents = [data.aws_iam_policy_document.reader.json]

  statement {
    sid       = "CommitToExistingTables"
    actions   = local.write_table_actions
    resources = [local.tables_arn]
  }

  dynamic "statement" {
    for_each = var.kms_key_arn == null ? [] : [var.kms_key_arn]
    content {
      sid       = "EncryptWithTableKey"
      actions   = ["kms:GenerateDataKey"]
      resources = [statement.value]
    }
  }
}

# ---------------------------------------------------------------- maintenance
# Configures the service's maintenance and reads its job status. It cannot
# read or write table data: the service runs the jobs, not this role.

data "aws_iam_policy_document" "maintenance" {
  statement {
    sid       = "Discover"
    actions   = local.discover_actions
    resources = [local.bucket_arn]
  }
  statement {
    sid       = "BucketMaintenance"
    actions   = local.maintenance_bucket_actions
    resources = [local.bucket_arn]
  }
  statement {
    sid       = "TableMaintenance"
    actions   = local.maintenance_table_actions
    resources = [local.tables_arn]
  }
}

# ---------------------------------------------------------------- admin
# Structure and policy, not data. Deliberately omits DeleteTableBucket:
# destroying the bucket is a break-glass action, not a role's standing right.

data "aws_iam_policy_document" "admin" {
  source_policy_documents = [data.aws_iam_policy_document.maintenance.json]

  statement {
    sid       = "AdminBucket"
    actions   = local.admin_bucket_actions
    resources = [local.bucket_arn]
  }
  statement {
    sid       = "AdminTables"
    actions   = local.admin_table_actions
    resources = [local.tables_arn]
  }
}

locals {
  role_policies = {
    reader      = data.aws_iam_policy_document.reader.json
    writer      = data.aws_iam_policy_document.writer.json
    maintenance = data.aws_iam_policy_document.maintenance.json
    admin       = data.aws_iam_policy_document.admin.json
  }
}

resource "aws_iam_policy" "role" {
  for_each    = local.role_policies
  name        = "${var.table_bucket_name}-${each.key}"
  description = "S3 Tables ${each.key} access to ${var.table_bucket_name} (Book 5 companion; never applied)."
  policy      = each.value
}

resource "aws_iam_role" "reader" {
  name               = "${var.table_bucket_name}-reader"
  assume_role_policy = data.aws_iam_policy_document.trust["reader"].json
}

resource "aws_iam_role" "writer" {
  name               = "${var.table_bucket_name}-writer"
  assume_role_policy = data.aws_iam_policy_document.trust["writer"].json
}

resource "aws_iam_role" "maintenance" {
  name               = "${var.table_bucket_name}-maintenance"
  assume_role_policy = data.aws_iam_policy_document.trust["maintenance"].json
}

resource "aws_iam_role" "admin" {
  name               = "${var.table_bucket_name}-admin"
  assume_role_policy = data.aws_iam_policy_document.trust["admin"].json
}

resource "aws_iam_role_policy_attachment" "role" {
  for_each = local.role_policies
  role = {
    reader      = aws_iam_role.reader.name
    writer      = aws_iam_role.writer.name
    maintenance = aws_iam_role.maintenance.name
    admin       = aws_iam_role.admin.name
  }[each.key]
  policy_arn = aws_iam_policy.role[each.key].arn
}
