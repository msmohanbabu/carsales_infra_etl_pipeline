# Databricks notebook source


import yaml
import boto3
import logging
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, sha2, concat_ws
from typing import Dict, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_credentials_from_secrets(
    spark,
    scope: str = "aws-credentials",
    key_prefix: str = "cloudwatch"
) -> dict:
    """Retrieve AWS credentials from Databricks secrets and return temporary session token."""
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)

    logger = logging.getLogger(__name__)

    try:
        # Base credentials from secrets
        credentials = {
            'aws_access_key_id': dbutils.secrets.get(scope, f"{key_prefix}-access-key-id"),
            'aws_secret_access_key': dbutils.secrets.get(scope, f"{key_prefix}-secret-access-key")
        }

        # Get temporary session token
        sts_client = boto3.client(
            'sts',
            aws_access_key_id=credentials['aws_access_key_id'],
            aws_secret_access_key=credentials['aws_secret_access_key']
        )

        response = sts_client.get_session_token(DurationSeconds=3600)
        temp = response['Credentials']

        credentials.update({
            'aws_access_key_id': temp['AccessKeyId'],
            'aws_secret_access_key': temp['SecretAccessKey'],
            'aws_session_token': temp['SessionToken']
        })

        logger.info("AWS credentials retrieved successfully")
        return credentials

    except Exception as e:
        logger.warning(f"Failed to retrieve AWS credentials: {e}")
        return {}

class SilverTransformationFramework:
    """
    Silver Layer Transformation Framework

    Features:
    - Metadata-driven SQL transformations
    - Multi-source joins and aggregations
    - Data quality validation
    - SCD Type 2 support
    - Audit column management
    """

    def __init__(self, spark, metadata_path: str):
        self.spark = spark
        self.metadata_path = metadata_path.rstrip('/')
        self.logger = logging.getLogger(__name__)

        # Get credentials from secrets
        credentials = get_credentials_from_secrets(spark)

        # Initialize S3 client with credentials
        if credentials:
            self.s3_client = boto3.client(
                's3',
                region_name='ap-southeast-2',
                aws_access_key_id=credentials.get('aws_access_key_id'),
                aws_secret_access_key=credentials.get('aws_secret_access_key'),
                aws_session_token=credentials.get('aws_session_token')
            )
        else:
            # Fallback to default credentials
            self.s3_client = boto3.client('s3', region_name='ap-southeast-2')

    def load_metadata(self, table_name: str) -> Dict:
        """Load YAML metadata for a table"""
        try:
            yaml_path = f"{self.metadata_path}/{table_name}/{table_name}.yaml"
            s3_path = yaml_path.replace('s3://', '')
            bucket = s3_path.split('/')[0]
            key = '/'.join(s3_path.split('/')[1:])

            self.logger.info(f"Loading metadata: {yaml_path}")
            self.logger.debug(f"S3 bucket: {bucket}, key: {key}")

            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            yaml_content = response['Body'].read().decode('utf-8')
            config = yaml.safe_load(yaml_content)

            if not config:
                raise ValueError(f"Empty metadata for {table_name}")

            config['table_name'] = table_name
            self.logger.info(f"Successfully loaded metadata for: {table_name}")
            return config

        except Exception as e:
            self.logger.error(f"Failed to load metadata for {table_name}: {e}")
            raise

    def execute_transformation(self, config: Dict) -> Optional[DataFrame]:
        """Execute SQL transformation from metadata"""
        table_name = config.get('table_name')
        transformation_config = config.get('transformation', {})
        sql_logic = transformation_config.get('logic', '').strip()

        if not sql_logic:
            raise ValueError(f"No transformation logic defined for {table_name}")

        self.logger.info(f"Executing transformation: {table_name}")
        self.logger.debug(f"Transformation type: {transformation_config.get('type', 'custom')}")

        try:
            # Execute SQL
            df = self.spark.sql(sql_logic)
            count = df.count()
            self.logger.info(f"Transformation complete: {count} records produced")
            return df

        except Exception as e:
            self.logger.error(f"Transformation failed for {table_name}: {e}")
            raise

    def add_audit_columns(self, df: DataFrame, config: Dict) -> DataFrame:
        """Add silver layer audit columns"""
        table_name = config.get('table_name')
        source_tables = config.get('source', {}).get('tables', [])

        self.logger.info(f"Adding audit columns for: {table_name}")

        # Try keys.primary_keys first, then schema.primary_keys
        primary_keys = config.get('primary_keys', [])
        self.logger.debug(f"Primary keys: {primary_keys}")

        if not primary_keys:
            primary_keys = config.get('schema', {}).get('primary_keys', [])

        # CDC key from primary keys
        if primary_keys:
            cdc_key_expr = sha2(
                concat_ws('|', *[col(pk).cast('string') for pk in primary_keys]), 256
            )
        else:
            cdc_key_expr = lit(None)

        df = df.withColumn('_processing_timestamp', current_timestamp()) \
               .withColumn('_data_quality_score', lit(None).cast('decimal(2,1)')) \
               .withColumn('_cdc_key', cdc_key_expr) \
               .withColumn('_is_current', lit(True)) \
               .withColumn('_effective_start_date', current_timestamp()) \
               .withColumn('_effective_end_date', lit(None).cast('timestamp')) \
               .withColumn('_created_by', lit('silver_framework')) \
               .withColumn('_updated_by', lit('silver_framework')) \
               .withColumn('_source_system', lit(','.join(source_tables)))

        # Checksum from business columns
        business_cols = [c for c in df.columns if not c.startswith('_')]
        checksum_expr = sha2(
            concat_ws('|', *[col(c).cast('string') for c in business_cols]), 256
        )
        df = df.withColumn('_checksum', checksum_expr)

        return df

    def apply_data_quality(self, df: DataFrame, config: Dict) -> Tuple[DataFrame, Dict]:
        """Apply data quality rules"""
        table_name = config.get('table_name')
        dq_config = config.get('data_quality', {})

        if not dq_config.get('enabled', False):
            count = df.count()
            return df, {'total': count, 'passed': count, 'rejected': 0, 'pass_rate': 100.0}

        rules = dq_config.get('rules', [])
        if not rules:
            count = df.count()
            return df, {'total': count, 'passed': count, 'rejected': 0, 'pass_rate': 100.0}

        self.logger.info(f"Applying {len(rules)} data quality rules")
        original_count = df.count()
        rejected_count = 0

        for rule in rules:
            rule_name = rule.get('name', 'unnamed')
            rule_type = rule.get('type')
            action = rule.get('action', 'reject')
            before = df.count()

            if rule_type == 'completeness' and action == 'reject':
                columns = rule.get('columns', [])
                for column in columns:
                    if column in df.columns:
                        df = df.filter(col(column).isNotNull())

            elif rule_type == 'validation' and action == 'reject':
                expression = rule.get('expression')
                if expression:
                    df = df.filter(expression)

            after = df.count()
            rejected = before - after
            if rejected > 0:
                self.logger.warning(f"Rule '{rule_name}' rejected {rejected} records")
                rejected_count += rejected

        final_count = df.count()
        pass_rate = (final_count / original_count * 100) if original_count > 0 else 0

        dq_stats = {
            'total': original_count,
            'passed': final_count,
            'rejected': rejected_count,
            'pass_rate': round(pass_rate, 2)
        }

        self.logger.info(f"Data quality complete: {pass_rate:.1f}% pass rate")
        return df, dq_stats

    def write_to_silver(self, df: DataFrame, config: Dict) -> int:
        """Write DataFrame to silver Delta table"""
        table_name = config.get('table_name')
        target_config = config.get('target', {})
        target_db = target_config.get('database', 'payment_analytics.silver')
        target_table = target_config.get('table')
        target_path = target_config.get('path')
        partition_cols = config.get('keys', {}).get('partition_columns', [])
        full_table_name = f"{target_db}.{target_table}"

        self.logger.info(f"Writing to silver table: {full_table_name}")

        # Create database if needed
        db_name = target_db.split('.')[-1] if '.' in target_db else target_db
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

        # Write to Delta with overwrite (full refresh for silver)
        writer = df.write.format('delta').mode('overwrite')
        writer = writer.option('overwriteSchema', 'true')

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        if target_path:
            writer = writer.option('path', target_path)

        writer.saveAsTable(full_table_name)

        record_count = df.count()
        self.logger.info(f"Successfully wrote {record_count} records to {full_table_name}")
        return record_count

    def transform_table(self, table_name: str) -> Dict:
        """Main transformation workflow"""
        self.logger.info("=" * 70)
        self.logger.info(f"STARTING TRANSFORMATION: {table_name}")
        self.logger.info("=" * 70)

        start_time = datetime.now()

        try:
            # Step 1: Load metadata
            config = self.load_metadata(table_name)

            if not config.get('enabled', True):
                self.logger.warning(f"Table {table_name} is disabled in metadata")
                return {
                    'table': table_name,
                    'status': 'SKIPPED',
                    'reason': 'Disabled in metadata',
                    'duration_seconds': 0
                }

            # Step 2: Execute transformation SQL
            transformed_df = self.execute_transformation(config)
            if transformed_df is None or transformed_df.count() == 0:
                self.logger.warning(f"No data produced for {table_name}")
                return {
                    'table': table_name,
                    'status': 'SKIPPED',
                    'reason': 'No data produced',
                    'duration_seconds': (datetime.now() - start_time).total_seconds()
                }

            # Step 3: Add audit columns
            df_with_audit = self.add_audit_columns(transformed_df, config)

            # Step 4: Apply data quality
            final_df, dq_stats = self.apply_data_quality(df_with_audit, config)

            # Step 5: Write to silver
            records_written = self.write_to_silver(final_df, config)

            duration = (datetime.now() - start_time).total_seconds()
            self.logger.info(f"SUCCESS: {table_name} | {records_written} records | {duration:.2f}s")
            self.logger.info("=" * 70)

            return {
                'table': table_name,
                'status': 'SUCCESS',
                'records': records_written,
                'duration_seconds': duration,
                'dq_stats': dq_stats
            }

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"FAILED: {table_name} - {e}")
            self.logger.info("=" * 70)
            return {
                'table': table_name,
                'status': 'FAILED',
                'error': str(e),
                'duration_seconds': duration
            }