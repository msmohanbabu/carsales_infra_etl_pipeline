output "mwaa_arn" {
  value = aws_mwaa_environment.this.arn
}

output "mwaa_name" {
  value = aws_mwaa_environment.this.name
}

output "mwaa_webserver_url" {
  value = aws_mwaa_environment.this.webserver_url
}
