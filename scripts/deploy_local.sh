#!/bin/bash
set -e

aws s3 sync config/metadata/ s3://${CONFIG_BUCKET}/metadata/ --delete
aws s3 sync config/pipeline/ s3://${CONFIG_BUCKET}/pipeline/ --delete

databricks workspace import_dir   databricks/notebooks   /Workspace/Shared/payment-pipeline   --overwrite

databricks workspace import_dir   databricks/framework   /Workspace/Shared/payment-pipeline/framework   --overwrite

aws s3 sync airflow/dags/ s3://${MWAA_BUCKET}/dags/ --exclude "*" --include "*.py"
aws s3 cp airflow/requirements.txt s3://${MWAA_BUCKET}/requirements.txt

echo "✅ Deployment complete!"
