terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# S3 Module - Creates MWAA bucket
module "s3" {
  source       = "../../modules/s3"
  project_name = var.project_name
  environment  = var.environment
  tags         = var.common_tags
}

# IAM Module - Creates MWAA execution role (MISSING!)
module "iam" {
  source            = "../../modules/iam"
  project_name      = var.project_name
  environment       = var.environment
  source_bucket_arn = module.s3.bucket_arn
  tags              = var.common_tags
  
  depends_on = [module.s3]  # Ensure S3 is created first
}

# MWAA Module - Creates Airflow environment
module "mwaa" {
  source             = "../../modules/mwaa"
  project_name       = var.project_name
  environment        = var.environment
  vpc_id             = var.vpc_id
  private_subnet_ids = var.private_subnet_ids
  airflow_version    = var.airflow_version
  source_bucket_arn  = module.s3.bucket_arn
  execution_role_arn = module.iam.mwaa_role_arn  # ADD THIS LINE
  environment_class  = var.mwaa_environment_class
  min_workers        = var.mwaa_min_workers
  max_workers        = var.mwaa_max_workers
  schedulers         = var.mwaa_schedulers
  tags               = var.common_tags
  
  depends_on = [module.iam]  # Ensure IAM role is created first
}

module "databricks" {
  source = "../../modules/databricks"
  
  project_name      = var.project_name
  environment       = var.environment
  databricks_host   = var.databricks_host
  databricks_token  = var.databricks_token
  
  tags = var.common_tags
}

# Secrets Manager - Databricks Connection
resource "aws_secretsmanager_secret" "databricks" {
  name                    = "airflow/connections/databricks_default"
  recovery_window_in_days = 0
  tags                    = var.common_tags
  
  lifecycle {
    ignore_changes = [name]
  }
}

resource "aws_secretsmanager_secret_version" "databricks" {
  secret_id = aws_secretsmanager_secret.databricks.id
  secret_string = jsonencode({
    conn_type = "databricks"
    host      = var.databricks_host
    extra     = jsonencode({ token = var.databricks_token })
  })
}