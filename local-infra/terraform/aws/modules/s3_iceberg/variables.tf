variable "environment" {
  type        = string
  description = "Deployment environment"
}

variable "project" {
  type        = string
  description = "Project prefix name"
}

variable "allow_destroy" {
  type        = bool
  description = <<-EOT
    Whether terraform destroy may delete the warehouse bucket and its contents.
    Set false in production; true is acceptable only for ephemeral dev /
    integration-test environments.
  EOT
  default     = false
}
