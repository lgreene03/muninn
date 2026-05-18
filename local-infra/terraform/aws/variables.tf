variable "aws_region" {
  type        = string
  description = "The target AWS region for deployment"
  default     = "us-east-1"
}

variable "environment" {
  type        = string
  description = "The target environment (e.g. staging, production)"
  default     = "production"
}

variable "project" {
  type        = string
  description = "Project name prefix for tags"
  default     = "muninn"
}

variable "vpc_cidr" {
  type        = string
  description = "The CIDR block for the VPC"
  default     = "10.0.0.0/16"
}

variable "eks_node_instance_types" {
  type        = list(string)
  description = "Instance types for the EKS node group"
  default     = ["m5.large"]
}

variable "eks_desired_capacity" {
  type        = number
  description = "Desired number of worker nodes in EKS"
  default     = 3
}

variable "msk_instance_type" {
  type        = string
  description = "Instance type for MSK broker nodes"
  default     = "kafka.t3.small"
}

variable "msk_broker_nodes" {
  type        = number
  description = "Number of broker nodes in the MSK cluster"
  default     = 3
}
