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
