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
  description = "Private subnets for node groups"
}

variable "node_instance_types" {
  type        = list(string)
  description = "Worker nodes EC2 instance sizes"
}

variable "node_desired_capacity" {
  type        = number
  description = "Worker nodes desired count"
}
