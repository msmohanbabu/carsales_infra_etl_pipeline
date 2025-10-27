variable "project_name" {
  type        = string
  description = "Project name"
}

variable "environment" {
  type        = string
  description = "Environment (dev/prod)"
}

variable "databricks_host" {
  type        = string
  description = "Databricks workspace URL"
}

variable "databricks_token" {
  type        = string
  sensitive   = true
  description = "Databricks access token"
}

variable "spark_version" {
  type        = string
  default     = "14.3.x-scala2.12"
  description = "Databricks runtime version"
}

variable "node_type_id" {
  type        = string
  default     = "m5.large"
  description = "Node type for cluster"
}

variable "autotermination_minutes" {
  type        = number
  default     = 20
  description = "Auto-termination time in minutes"
}

variable "min_workers" {
  type        = number
  default     = 1
  description = "Minimum number of workers"
}

variable "max_workers" {
  type        = number
  default     = 3
  description = "Maximum number of workers"
}

variable "tags" {
  type        = map(string)
  default     = {}
  description = "Tags for resources"
}
