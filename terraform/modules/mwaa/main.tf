data "aws_vpc" "existing" {
  id = var.vpc_id
}

resource "aws_security_group" "mwaa" {
  name_prefix = "${var.project_name}-mwaa-sg-"
  vpc_id      = data.aws_vpc.existing.id
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-mwaa-sg-${var.environment}"
  })
  
  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_mwaa_environment" "this" {
  name               = "${var.project_name}-mwaa-${var.environment}"
  airflow_version    = var.airflow_version
  execution_role_arn = var.execution_role_arn  # ONLY ONE - use the variable
  source_bucket_arn  = var.source_bucket_arn
  dag_s3_path        = "dags/"
  plugins_s3_path    = "plugins/"
  
  network_configuration {
    security_group_ids = [aws_security_group.mwaa.id]
    subnet_ids         = var.private_subnet_ids
  }
  
  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "INFO"
    }
    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }
  
  environment_class     = var.environment_class
  min_workers           = var.min_workers
  max_workers           = var.max_workers
  schedulers            = var.schedulers
  webserver_access_mode = var.webserver_access_mode
  
  airflow_configuration_options = {
    "core.default_task_retries"     = "3"
    "core.parallelism"              = "32"
    "scheduler.catchup_by_default"  = "False"
    "webserver.default_ui_timezone" = "Australia/Sydney"
  }
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-mwaa-${var.environment}"
  })
}