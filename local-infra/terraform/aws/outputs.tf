output "vpc_id" {
  value       = module.vpc.vpc_id
  description = "The ID of the VPC created"
}

output "eks_cluster_endpoint" {
  value       = module.eks.cluster_endpoint
  description = "The endpoint for the EKS Kubernetes control plane"
}

output "msk_bootstrap_brokers" {
  value       = module.msk.bootstrap_brokers
  description = "Connection string for the Managed Kafka brokers"
}

output "s3_warehouse_bucket_name" {
  value       = module.s3_iceberg.bucket_name
  description = "The S3 bucket name designated for the Iceberg Parquet warehouse"
}

output "glue_catalog_database_name" {
  value       = module.s3_iceberg.glue_database_name
  description = "The AWS Glue metadata database catalog name"
}
