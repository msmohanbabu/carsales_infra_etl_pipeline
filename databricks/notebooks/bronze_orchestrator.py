# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %run /Users/connectmohanms@gmail.com/notebooks/bronze_ingestion_framework

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer Orchestrator
# MAGIC Main driver for Bronze layer ingestion with metadata-driven processing.

# COMMAND ----------

import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text(
    "metadata_path",
    "s3://databricks-workspace-stack-5df43-bucket/unity-catalog/4227733217617218/metadata/bronze/",
    "Metadata S3 Path"
)

dbutils.widgets.text(
    "table_list",
    "invoices,accounts,skus,invoice_line_items",
    "Comma-separated table names (or 'all')"
)

dbutils.widgets.text("environment", "dev", "Environment (dev/uat/prod)")

# Get parameters
METADATA_PATH = dbutils.widgets.get("metadata_path")
TABLE_LIST = dbutils.widgets.get("table_list")
ENVIRONMENT = dbutils.widgets.get("environment")

logger.info("Bronze Orchestrator Configuration:")
logger.info(f"  Metadata Path: {METADATA_PATH}")
logger.info(f"  Tables: {TABLE_LIST}")
logger.info(f"  Environment: {ENVIRONMENT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Framework with AWS Credentials from Secrets

# COMMAND ----------

from bronze_ingestion_framework import get_credentials_from_secrets

try:
    # Get AWS credentials from Databricks secrets
    aws_credentials = get_credentials_from_secrets(
        spark,
        scope="aws-credentials",
        key_prefix="cloudwatch"
    )

    if aws_credentials:
        logger.info("AWS credentials retrieved successfully from secrets")
    else:
        logger.warning("Using default AWS credentials")
        aws_credentials = None

except Exception as e:
    logger.warning(f"Failed to retrieve AWS credentials: {e}")
    logger.info("Proceeding with default credentials")
    aws_credentials = None

# Initialize framework
framework = BronzeIngestionFramework(
    spark=spark,
    metadata_path=METADATA_PATH,
    logger=logger,
    aws_credentials=aws_credentials
)

logger.info("Bronze ingestion framework initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Tables

# COMMAND ----------

def get_table_list(table_input: str) -> list:
    """Parse table input and return list of tables"""
    if table_input.lower() == 'all':
        logger.info("Processing mode: ALL tables")
        return ['invoices', 'accounts', 'skus', 'invoice_line_items']
    else:
        tables = [t.strip() for t in table_input.split(',') if t.strip()]
        logger.info(f"Processing mode: SELECTED tables ({len(tables)})")
        return tables

# Get tables to process
tables_to_process = get_table_list(TABLE_LIST)
logger.info(f"Tables to process: {tables_to_process}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Ingestion

# COMMAND ----------

start_time = datetime.now()
results = []

logger.info("=" * 80)
logger.info("BRONZE LAYER INGESTION STARTING")
logger.info("=" * 80)

for table_name in tables_to_process:
    try:
        logger.info(f"Processing table: {table_name}")
        result = framework.ingest_table(table_name)
        results.append(result)

        if result['status'] == 'SUCCESS':
            logger.info(f"Completed: {table_name} - {result.get('records', 0)} records")
        elif result['status'] == 'SKIPPED':
            logger.warning(f"Skipped: {table_name} - {result.get('reason', 'Unknown')}")
        else:
            logger.error(f"Failed: {table_name} - {result.get('error', 'Unknown error')}")

    except Exception as e:
        logger.error(f"Unexpected error processing {table_name}: {e}")
        results.append({
            'table': table_name,
            'status': 'FAILED',
            'error': str(e)
        })

total_duration = (datetime.now() - start_time).total_seconds()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Report

# COMMAND ----------

logger.info("=" * 80)
logger.info("BRONZE LAYER INGESTION SUMMARY")
logger.info("=" * 80)

success_count = sum(1 for r in results if r['status'] == 'SUCCESS')
failed_count = sum(1 for r in results if r['status'] == 'FAILED')
skipped_count = sum(1 for r in results if r['status'] == 'SKIPPED')
total_records = sum(r.get('records', 0) for r in results if r['status'] == 'SUCCESS')

logger.info(f"Total Tables: {len(results)}")
logger.info(f"  Success: {success_count}")
logger.info(f"  Failed: {failed_count}")
logger.info(f"  Skipped: {skipped_count}")
logger.info(f"Total Records: {total_records}")
logger.info(f"Total Duration: {total_duration:.2f}s")

# Detailed results
logger.info("\nDetailed Results:")
for result in results:
    status = result['status']
    table = result['table']

    if status == 'SUCCESS':
        records = result.get('records', 0)
        duration = result.get('duration_seconds', 0)
        dq_stats = result.get('dq_stats', {})
        pass_rate = dq_stats.get('pass_rate', 100)
        logger.info(f"  {table}: SUCCESS | {records} records | {duration:.2f}s | DQ: {pass_rate}%")
    elif status == 'FAILED':
        error = result.get('error', 'Unknown')
        logger.error(f"  {table}: FAILED | {error}")
    else:
        reason = result.get('reason', 'Unknown')
        logger.warning(f"  {table}: SKIPPED | {reason}")

logger.info("=" * 80)

if failed_count > 0:
    logger.error(f"Ingestion completed with {failed_count} failures")
else:
    logger.info("Ingestion completed successfully")