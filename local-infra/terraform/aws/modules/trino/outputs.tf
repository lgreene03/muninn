output "security_group_id" {
  value = aws_security_group.trino.id
}

output "iam_policy_arn" {
  value = aws_iam_policy.trino_s3_glue_access.arn
}
