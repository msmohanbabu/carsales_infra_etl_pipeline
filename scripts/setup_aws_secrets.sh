#!/bin/bash
set -e

export DATABRICKS_HOST="https://dbc-a7d34ef8-2769.cloud.databricks.com"
export DATABRICKS_TOKEN="dapieea7b33c530d742cdf2f8984029e303a"

AWS_REGION="ap-southeast-2"

echo "1. Creating Databricks connection secret..."
aws secretsmanager create-secret   --name "airflow/connections/databricks_default"   --description "Databricks connection for MWAA"   --secret-string '{
    "conn_type": "databricks",
    "host": "https://dbc-a7d34ef8-2769.cloud.databricks.com",
    "extra": "{\"token\": \"dapieea7b33c530d742cdf2f8984029e303a\"}"
  }'   --region $AWS_REGION || echo "Secret already exists"

echo "2. Creating CloudWatch credentials secret..."
aws secretsmanager create-secret   --name "databricks/cloudwatch/credentials"   --description "CloudWatch credentials for Databricks"   --secret-string '{
    "aws_access_key_id": "AKIAZLLU3CMALA7CM4HT",
    "aws_secret_access_key": "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"
  }'   --region $AWS_REGION || echo "Secret already exists"

