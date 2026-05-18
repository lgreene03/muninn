variable "vpc_cidr" {
  type        = string
  description = "CIDR block for the VPC"
}

variable "environment" {
  type        = string
  description = "Deployment environment"
}

variable "project" {
  type        = string
  description = "Project name prefix"
}
