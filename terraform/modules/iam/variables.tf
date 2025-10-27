variable "project_name" {
  type        = string
  description = "Project name for resource naming"
}

variable "environment" {
  type        = string
  description = "Environment name (dev/staging/prod)"
}

variable "source_bucket_arn" {
  type        = string
  description = "ARN of the S3 bucket for MWAA"
}

variable "tags" {
  type        = map(string)
  default     = {}
  description = "Common tags for all resources"
}