output "mwaa_role_arn" {
  value       = aws_iam_role.mwaa.arn
  description = "MWAA execution role ARN"
}

output "mwaa_role_name" {
  value       = aws_iam_role.mwaa.name
  description = "MWAA execution role name"
}