project_name = "carsales-payment"
environment  = "dev"
aws_region   = "ap-southeast-2"

vpc_id = "vpc-0fbbf5e54c3fb84fd"
private_subnet_ids = [
  "subnet-0956500082b70c400",
  "subnet-09d06da6245349066"
]

airflow_version        = "2.7.2"
mwaa_environment_class = "mw1.small"
mwaa_min_workers       = 1
mwaa_max_workers       = 10
mwaa_schedulers        = 2
databricks_host = "https://dbc-a7d34ef8-2769.cloud.databricks.com"
databricks_token = "dapieea7b33c530d742cdf2f8984029e303a"

common_tags = {
  Project     = "Payment Pipeline"
  Environment = "dev"
  ManagedBy   = "Terraform"
}