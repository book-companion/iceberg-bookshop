# A read-only replica of the orders table in a second Region.
# The role's permissions are not written here: supply a role you have
# built from AWS's replication documentation.

provider "aws" {
  alias  = "replica"
  region = var.replica_region
}

variable "replica_region" {
  type    = string
  default = "us-west-2"
}

variable "replication_role_arn" {
  type = string
}

resource "aws_s3tables_table_bucket" "bookshop_replica" {
  provider = aws.replica
  name     = "${var.table_bucket_name}-replica"
}

resource "aws_s3tables_table_replication" "orders" {
  table_arn = aws_s3tables_table.orders.arn
  role      = var.replication_role_arn

  rule {
    destination {
      destination_table_bucket_arn = aws_s3tables_table_bucket.bookshop_replica.arn
    }
  }
}
