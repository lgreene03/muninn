resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

# Warehouse bucket — holds Parquet files (today) and Iceberg metadata (Phase 8).
#
# Hardening:
#   - server-side encryption (AES256) enforced.
#   - versioning enabled so accidental deletes / overwrites are recoverable.
#   - lifecycle policy transitions older partitions to cheaper storage tiers
#     and expires non-current versions to bound cost.
#   - public access blocked at every available layer.
#   - force_destroy is gated on the environment so production resists
#     destroy-everything terraform commands.

resource "aws_s3_bucket" "warehouse" {
  bucket        = "${var.project}-${var.environment}-warehouse-${random_string.suffix.result}"
  force_destroy = var.allow_destroy

  tags = {
    Name = "${var.project}-${var.environment}-warehouse"
  }
}

resource "aws_s3_bucket_public_access_block" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_versioning" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id

  rule {
    id     = "transition-cold-partitions"
    status = "Enabled"

    filter {
      prefix = ""
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
  }

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    filter {
      prefix = ""
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# Glue catalog database — Iceberg table metadata lives here.
# Trino, Spark, Flink, and the Muninn application all share this catalog.
resource "aws_glue_catalog_database" "iceberg" {
  name        = "${var.project}_${var.environment}_catalog"
  description = "AWS Glue Catalog for Muninn Apache Iceberg tables metadata"
}
