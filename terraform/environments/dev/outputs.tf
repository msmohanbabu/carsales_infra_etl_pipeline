output "mwaa_url" {
  value = module.mwaa.mwaa_webserver_url
}

output "s3_bucket" {
  value = module.s3.bucket_name
}

output "databricks_cluster_id" {
  value       = module.databricks.cluster_id
  description = "Databricks cluster ID"
}

output "bronze_job_id" {
  value       = module.databricks.bronze_job_id
  description = "Bronze job ID"
}

output "silver_job_id" {
  value       = module.databricks.silver_job_id
  description = "Silver job ID"
}

output "gold_job_id" {
  value       = module.databricks.gold_job_id
  description = "Gold job ID"
}