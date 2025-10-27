variable "aws_region" {
  type    = string
  default = "ap-southeast-2"
}

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

variable "mwaa_environment_class" {
  type    = string
  default = "mw1.small"
}

variable "mwaa_min_workers" {
  type    = number
  default = 1
}

variable "mwaa_max_workers" {
  type    = number
  default = 10
}

variable "mwaa_schedulers" {
  type    = number
  default = 2
}

variable "databricks_host" {
  type      = string
  sensitive = true
}

variable "databricks_token" {
  type      = string
  sensitive = true
}

variable "common_tags" {
  type    = map(string)
  default = {}
}