output "cluster_id" {
  value       = databricks_cluster.payment_cluster.id
  description = "Databricks cluster ID"
}

output "bronze_job_id" {
  value       = databricks_job.bronze_job.id
  description = "Bronze layer job ID"
}

output "silver_job_id" {
  value       = databricks_job.silver_job.id
  description = "Silver layer job ID"
}

output "gold_job_id" {
  value       = databricks_job.gold_job.id
  description = "Gold layer job ID"
}