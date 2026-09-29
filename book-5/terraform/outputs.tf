output "table_bucket_arn" {
  description = "Warehouse value for Iceberg REST clients."
  value       = aws_s3tables_table_bucket.bookshop.arn
}

output "iceberg_rest_uri" {
  description = "The S3 Tables Iceberg REST endpoint for this Region (SigV4 signing name s3tables)."
  value       = "https://s3tables.${var.region}.amazonaws.com/iceberg"
}

output "orders_table_arn" {
  value = aws_s3tables_table.orders.arn
}

output "role_arns" {
  value = {
    reader      = aws_iam_role.reader.arn
    writer      = aws_iam_role.writer.arn
    maintenance = aws_iam_role.maintenance.arn
    admin       = aws_iam_role.admin.arn
  }
}
