resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

resource "aws_s3_bucket" "warehouse" {
  bucket        = "${var.project}-${var.environment}-warehouse-${random_string.suffix.result}"
  force_destroy = true

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

resource "aws_glue_catalog_database" "iceberg" {
  name        = "${var.project}_${var.environment}_catalog"
  description = "AWS Glue Catalog for Muninn Apache Iceberg tables metadata"
}
