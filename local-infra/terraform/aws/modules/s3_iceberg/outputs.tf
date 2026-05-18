output "bucket_name" {
  value = aws_s3_bucket.warehouse.bucket
}

output "bucket_arn" {
  value = aws_s3_bucket.warehouse.arn
}

output "glue_database_name" {
  value = aws_glue_catalog_database.iceberg.name
}
