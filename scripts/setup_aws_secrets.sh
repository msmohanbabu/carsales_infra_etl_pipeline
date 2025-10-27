#!/bin/bash
set -e

export DATABRICKS_HOST="https://dbc-a7d34ef8-2769.cloud.databricks.com"
export DATABRICKS_TOKEN="XXXXXXX"

AWS_REGION="ap-southeast-2"

echo "1. Creating Databricks connection secret..."
aws secretsmanager create-secret   --name "airflow/connections/databricks_default"   --description "Databricks connection for MWAA"   --secret-string '{
    "conn_type": "databricks",
    "host": "https://dbc-a7d34ef8-2769.cloud.databricks.com",
    "extra": "{\"token\": \"XXXXXXXX\"}"
  }'   --region $AWS_REGION || echo "Secret already exists"

echo "2. Creating CloudWatch credentials secret..."
aws secretsmanager create-secret   --name "databricks/cloudwatch/credentials"   --description "CloudWatch credentials for Databricks"   --secret-string '{
    "aws_access_key_id": "XXXXXX",
    "aws_secret_access_key": "XXXX"
  }'   --region $AWS_REGION || echo "Secret already exists"

