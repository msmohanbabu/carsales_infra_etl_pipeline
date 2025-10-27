terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

# Create Cluster
resource "databricks_cluster" "payment_cluster" {
  cluster_name            = "${var.project_name}-cluster"
  spark_version           = var.spark_version
  node_type_id            = var.node_type_id
  autotermination_minutes = var.autotermination_minutes
  
  autoscale {
    min_workers = var.min_workers
    max_workers = var.max_workers
  }
  
  aws_attributes {
    availability           = "SPOT_WITH_FALLBACK"
    zone_id                = "auto"
    spot_bid_price_percent = 50
    ebs_volume_count = 1
    ebs_volume_size  = 50
  }
  
  spark_conf = {
    "spark.databricks.delta.preview.enabled" = "true"
  }
  
  custom_tags = var.tags
}

# Bronze Layer Job
resource "databricks_job" "bronze_job" {
  name = "${var.project_name}-bronze-ingestion"

  task {
    task_key = "bronze_ingestion"
    
    existing_cluster_id = databricks_cluster.payment_cluster.id
    
    notebook_task {
      notebook_path = "/Workspace/bronze_ingestion"
      base_parameters = {
        environment = var.environment
        layer       = "bronze"
      }
    }
  }
  
  tags = var.tags
}

# Silver Layer Job
resource "databricks_job" "silver_job" {
  name = "${var.project_name}-silver-transformation"

  task {
    task_key = "silver_transformation"
    
    existing_cluster_id = databricks_cluster.payment_cluster.id
    
    notebook_task {
      notebook_path = "/Workspace/silver_transformation"
      base_parameters = {
        environment = var.environment
        layer       = "silver"
      }
    }
  }
  
  tags = var.tags
}

# Gold Layer Job
resource "databricks_job" "gold_job" {
  name = "${var.project_name}-gold-aggregation"

  task {
    task_key = "gold_aggregation"
    
    existing_cluster_id = databricks_cluster.payment_cluster.id
    
    notebook_task {
      notebook_path = "/Workspace/gold_aggregation"
      base_parameters = {
        environment = var.environment
        layer       = "gold"
      }
    }
  }
  
  tags = var.tags
}