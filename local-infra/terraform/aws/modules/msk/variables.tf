variable "environment" {
  type        = string
  description = "Deployment environment"
}

variable "project" {
  type        = string
  description = "Project prefix name"
}

variable "vpc_id" {
  type        = string
  description = "VPC ID from VPC module"
}

variable "private_subnet_ids" {
  type        = list(string)
  description = "Private subnets for MSK brokers placement"
}

variable "instance_type" {
  type        = string
  description = "MSK broker instance type"
}

variable "broker_nodes" {
  type        = number
  description = "Count of broker nodes in cluster"
}

variable "kafka_version" {
  type        = string
  description = "Kafka version. Bump per AWS-supported list; the application is wire-compatible across recent minor versions."
  default     = "3.6.0"
}

variable "broker_volume_size_gb" {
  type        = number
  description = "Per-broker EBS volume size in GB. 100 GB suits the BTC-USDT MVP retention; bump for multi-instrument scale-up."
  default     = 100
}

variable "additional_ingress_cidrs" {
  type        = list(string)
  description = <<-EOT
    Extra CIDR blocks permitted to reach Kafka ports beyond the VPC CIDR.
    Use sparingly — peered VPCs, VPN tunnels. The default (empty) means
    in-VPC clients only.
  EOT
  default     = []
}

variable "kms_key_arn" {
  type        = string
  description = <<-EOT
    ARN of a customer-managed KMS key for at-rest broker encryption.
    Null uses the AWS-managed key (aws/kafka). Override when compliance
    requires a CMK with a controlled rotation schedule.
  EOT
  default     = null
}

variable "log_retention_days" {
  type        = number
  description = "CloudWatch retention for broker logs. Defaults to 14 days; bump for audit-driven environments."
  default     = 14
}

variable "enhanced_monitoring" {
  type        = string
  description = <<-EOT
    MSK enhanced monitoring level. PER_BROKER is enough to populate the
    pipeline-overview dashboard from OBSERVABILITY_STRATEGY.md; bump to
    PER_TOPIC_PER_BROKER when topic-level lag dashboards are needed.
  EOT
  default     = "PER_BROKER"
  validation {
    condition     = contains(["DEFAULT", "PER_BROKER", "PER_TOPIC_PER_BROKER", "PER_TOPIC_PER_PARTITION"], var.enhanced_monitoring)
    error_message = "enhanced_monitoring must be DEFAULT, PER_BROKER, PER_TOPIC_PER_BROKER, or PER_TOPIC_PER_PARTITION."
  }
}
