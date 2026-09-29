variable "region" {
  description = "AWS Region for the table bucket."
  type        = string
  default     = "us-east-1"
}

variable "table_bucket_name" {
  description = "Table bucket name: unique within the account and Region."
  type        = string
  default     = "bookshop-tables"
}

variable "namespace" {
  description = "Namespace that holds the bookshop tables."
  type        = string
  default     = "bookshop"
}

variable "kms_key_arn" {
  description = "Customer managed KMS key for SSE-KMS. Null keeps the default, SSE-S3."
  type        = string
  default     = null
}

variable "trusted_principal_arns" {
  description = "IAM principals (roles or users in this account) allowed to assume each role."
  type = object({
    reader      = list(string)
    writer      = list(string)
    maintenance = list(string)
    admin       = list(string)
  })
}

# The KMS key itself is not created here. Its key policy must also allow the
# service principal maintenance.s3tables.amazonaws.com kms:GenerateDataKey and
# kms:Decrypt, or creating an SSE-KMS table fails (see ../README.md).
