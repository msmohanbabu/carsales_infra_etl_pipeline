variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "airflow_version" {
  type    = string
  default = "2.7.2"
}

variable "source_bucket_arn" {
  type = string
}

variable "environment_class" {
  type    = string
  default = "mw1.small"
}

variable "min_workers" {
  type    = number
  default = 1
}

variable "max_workers" {
  type    = number
  default = 10
}

variable "schedulers" {
  type    = number
  default = 2
}

variable "webserver_access_mode" {
  type    = string
  default = "PUBLIC_ONLY"
}

variable "tags" {
  type    = map(string)
  default = {}
}

variable "execution_role_arn" {
  type        = string
  description = "IAM role ARN for MWAA execution"
}