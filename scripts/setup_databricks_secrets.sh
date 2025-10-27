#!/bin/bash
set -e

echo "Setting up Databricks Secrets..."

export DATABRICKS_HOST="https://dbc-a7d34ef8-2769.cloud.databricks.com"
export DATABRICKS_TOKEN="dapieea7b33c530d742cdf2f8984029e303a"


echo "Databricks Host: $DATABRICKS_HOST"

databricks configure --token <<EOF
$DATABRICKS_HOST
$DATABRICKS_TOKEN
EOF

databricks secrets create-scope --scope aws-credentials --yes || echo "Scope exists"

databricks secrets put --scope aws-credentials --key cloudwatch-access-key-id
databricks secrets put --scope aws-credentials --key cloudwatch-secret-access-key

