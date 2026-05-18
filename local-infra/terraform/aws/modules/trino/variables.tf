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
  description = "Private subnets for security group placement"
}

variable "eks_cluster_name" {
  type        = string
  description = "Name of EKS cluster"
}
