# One table bucket, one namespace, one Iceberg table: the bookshop's orders.
# Maintenance values are written out even where they equal the documented
# defaults, so a reader can see every knob the service exposes.

resource "aws_s3tables_table_bucket" "bookshop" {
  name = var.table_bucket_name

  encryption_configuration = var.kms_key_arn == null ? {
    sse_algorithm = "AES256"
    kms_key_arn   = null
    } : {
    sse_algorithm = "aws:kms"
    kms_key_arn   = var.kms_key_arn
  }

  # Bucket-level: removes files no snapshot references.
  maintenance_configuration = {
    iceberg_unreferenced_file_removal = {
      status = "enabled"
      settings = {
        unreferenced_days = 3
        non_current_days  = 10
      }
    }
  }
}

resource "aws_s3tables_namespace" "bookshop" {
  namespace        = var.namespace
  table_bucket_arn = aws_s3tables_table_bucket.bookshop.arn
}

resource "aws_s3tables_table" "orders" {
  name             = "orders"
  namespace        = aws_s3tables_namespace.bookshop.namespace
  table_bucket_arn = aws_s3tables_table_bucket.bookshop.arn
  format           = "ICEBERG"

  metadata {
    iceberg {
      schema {
        field {
          name     = "order_id"
          type     = "long"
          required = true
        }
        field {
          name     = "customer_id"
          type     = "long"
          required = true
        }
        field {
          name = "order_ts"
          type = "timestamptz"
        }
        field {
          name = "total"
          type = "decimal(10,2)"
        }
        field {
          name = "status"
          type = "string"
        }
      }
    }
  }

  # Table-level: compaction and snapshot management.
  maintenance_configuration = {
    iceberg_compaction = {
      status = "enabled"
      settings = {
        target_file_size_mb = 512
      }
    }
    iceberg_snapshot_management = {
      status = "enabled"
      settings = {
        min_snapshots_to_keep  = 1
        max_snapshot_age_hours = 120
      }
    }
  }
}
