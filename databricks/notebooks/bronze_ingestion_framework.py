# Databricks notebook source

import yaml
import boto3
import logging
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, lit, sha2, concat_ws,
    year, month, dayofmonth, expr, to_date, regexp_replace, trim
)
from typing import Dict, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class BronzeIngestionFramework:
    """
    Bronze Layer Ingestion Framework

    Features:
    - Metadata-driven configuration from S3 YAML files
    - Automatic column casting from metadata types
    - SCD Type 2 change tracking
    - Data quality rule application
    - Audit column management
    - Delta table writes with partitioning
    """

    def __init__(self, spark, metadata_path: str, logger=None, aws_credentials: dict = None):
        self.spark = spark
        self.metadata_path = metadata_path.rstrip('/')
        self.logger = logger or logging.getLogger(__name__)
        self.aws_credentials = aws_credentials

        # Initialize S3 client with credentials if available
        if self.aws_credentials:
            self.s3_client = boto3.client(
                's3',
                region_name='ap-southeast-2',
                aws_access_key_id=self.aws_credentials.get('aws_access_key_id'),
                aws_secret_access_key=self.aws_credentials.get('aws_secret_access_key'),
                aws_session_token=self.aws_credentials.get('aws_session_token')
            )
        else:
            self.s3_client = boto3.client('s3', region_name='ap-southeast-2')

    def load_metadata(self, table_name: str) -> Dict:
        """Load YAML metadata for a table"""
        try:
            yaml_path = f"{self.metadata_path}/{table_name}/{table_name}.yaml"
            s3_path = yaml_path.replace('s3://', '')
            bucket = s3_path.split('/')[0]
            key = '/'.join(s3_path.split('/')[1:])

            self.logger.info(f"Loading metadata: {table_name}")
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            yaml_content = response['Body'].read().decode('utf-8')
            config = yaml.safe_load(yaml_content)

            if not config:
                raise ValueError(f"Empty metadata for {table_name}")

            config['table_name'] = table_name
            self.logger.info(f"Loaded metadata for: {table_name}")
            return config

        except Exception as e:
            self.logger.error(f"Failed to load metadata for {table_name}: {e}")
            raise

    def load_source_data(self, config: Dict) -> Optional[DataFrame]:
        """Load source data from configured path"""
        table_name = config.get('table_name')
        source_config = config.get('source', {})
        source_type = source_config.get('type', 'csv')
        source_path = source_config.get('path')
        source_format = source_config.get('format', source_type)
        options = source_config.get('options', {})

        self.logger.info(f"Loading source data: {table_name}")

        try:
            reader = self.spark.read.format(source_format)
            for key, value in options.items():
                reader = reader.option(key, value)

            df = reader.load(source_path)

            # Clean multiline fields (remove newlines from text columns)
            if source_config.get('clean_multiline', True):
                for col_name in df.columns:
                    df = df.withColumn(col_name, 
                                     trim(regexp_replace(col(col_name), "\\n", " ")))

            count = df.count()
            self.logger.info(f"Loaded {count} records for: {table_name}")
            return df

        except Exception as e:
            self.logger.warning(f"No source data for {table_name}: {e}")
            return None

    def cast_columns_to_metadata_types(self, df: DataFrame, config: Dict) -> DataFrame:
        """
        Cast columns to data types defined in metadata YAML

        Supports both formats:
        - columns: [{name: x, type: y}] (list format)
        - schema.columns: {col_name: {type: x, format: y}} (dict format)

        Uses try_cast to handle conversion errors gracefully
        Handles date formats using to_date when format is specified
        """
        # Try schema.columns format first (dictionary)
        schema_config = config.get('schema', {})
        columns_dict = schema_config.get('columns', {})

        if columns_dict and isinstance(columns_dict, dict):
            self.logger.info(f"Casting {len(columns_dict)} columns from schema.columns")
            for col_name, col_def in columns_dict.items():
                col_type = col_def.get('type')
                col_format = col_def.get('format')

                if col_name in df.columns and col_type:
                    if col_type == 'date' and col_format:
                        self.logger.debug(f"Casting '{col_name}' to date with format {col_format}")
                        df = df.withColumn(col_name, to_date(col(col_name), col_format))
                    else:
                        self.logger.debug(f"Casting '{col_name}' to {col_type}")
                        df = df.withColumn(col_name, expr(f"try_cast({col_name} as {col_type})"))
            return df

        # Fallback to columns format (list)
        columns_config = config.get('columns', [])
        if not columns_config:
            self.logger.info("No column type definitions in metadata")
            return df

        self.logger.info(f"Casting {len(columns_config)} columns to metadata types")
        for col_def in columns_config:
            col_name = col_def.get('name')
            col_type = col_def.get('type')
            col_format = col_def.get('format')

            if col_name and col_type and col_name in df.columns:
                if col_type == 'date' and col_format:
                    self.logger.debug(f"Casting '{col_name}' to date with format {col_format}")
                    df = df.withColumn(col_name, to_date(col(col_name), col_format))
                else:
                    self.logger.debug(f"Casting '{col_name}' to {col_type}")
                    df = df.withColumn(col_name, expr(f"try_cast({col_name} as {col_type})"))

        return df

    def add_audit_columns(self, df: DataFrame, config: Dict) -> DataFrame:
        """Add audit and tracking columns"""
        table_name = config.get('table_name')
        source_file = config.get('source', {}).get('path', 'unknown')

        # Try keys.primary_keys first, then schema.primary_keys
        primary_keys = config.get('keys', {}).get('primary_keys', [])
        if not primary_keys:
            primary_keys = config.get('schema', {}).get('primary_keys', [])

        self.logger.info(f"Adding audit columns: {table_name}")

        # CDC key from primary keys
        if primary_keys:
            cdc_key_expr = sha2(
                concat_ws('|', *[col(pk).cast('string') for pk in primary_keys]), 256
            )
        else:
            cdc_key_expr = lit(None)

        df = df.withColumn('_cdc_key', cdc_key_expr) \
               .withColumn('_ingestion_timestamp', current_timestamp()) \
               .withColumn('_source_file', lit(source_file)) \
               .withColumn('_is_current', lit(True)) \
               .withColumn('_effective_start_date', current_timestamp()) \
               .withColumn('_effective_end_date', lit(None).cast('timestamp')) \
               .withColumn('_created_by', lit('bronze_framework')) \
               .withColumn('_updated_by', lit('bronze_framework'))

        # Checksum from business columns
        business_cols = [c for c in df.columns if not c.startswith('_')]
        checksum_expr = sha2(
            concat_ws('|', *[col(c).cast('string') for c in business_cols]), 256
        )
        df = df.withColumn('_checksum', checksum_expr)

        return df

    def apply_scd_type2(self, new_df: DataFrame, config: Dict) -> DataFrame:
        """Apply SCD Type 2 logic"""
        table_name = config.get('table_name')
        target_config = config.get('target', {})
        target_table = f"{target_config.get('database')}.{target_config.get('table')}"

        # Try keys.primary_keys first, then schema.primary_keys
        primary_keys = config.get('keys', {}).get('primary_keys', [])
        if not primary_keys:
            primary_keys = config.get('schema', {}).get('primary_keys', [])

        self.logger.info(f"Applying SCD Type 2: {table_name}")

        try:
            if not self.spark.catalog.tableExists(target_table):
                self.logger.info(f"Initial load for {table_name}")
                return new_df

            existing_df = self.spark.table(target_table).filter(col('_is_current') == True)

            if existing_df.count() == 0:
                self.logger.info(f"No existing records for {table_name}")
                return new_df

            # Join and find changes
            join_condition = [new_df[pk] == existing_df[pk] for pk in primary_keys]
            joined_df = new_df.alias('new').join(existing_df.alias('existing'), join_condition, 'left')

            changed_df = joined_df.filter(
                (col('existing._checksum').isNotNull()) &
                (col('new._checksum') != col('existing._checksum'))
            ).select('new.*')

            new_records_df = joined_df.filter(col('existing._checksum').isNull()).select('new.*')

            changed_count = changed_df.count()
            if changed_count > 0:
                self.logger.info(f"Found {changed_count} changed records")

                # Close old versions
                pk_values = changed_df.select(*primary_keys).distinct().collect()
                close_conditions = []
                for pk_vals in pk_values:
                    pk_condition = " AND ".join([
                        f"{pk} = '{pk_vals[pk]}'" for pk in primary_keys
                    ])
                    close_conditions.append(f"({pk_condition})")

                close_condition = " OR ".join(close_conditions)
                self.spark.sql(f"""
                    UPDATE {target_table}
                    SET _is_current = false,
                        _effective_end_date = current_timestamp(),
                        _updated_by = 'scd_type2'
                    WHERE _is_current = true AND ({close_condition})
                """)

            result_df = new_records_df.union(changed_df)
            self.logger.info(f"SCD Type 2 complete: {result_df.count()} records to insert")
            return result_df

        except Exception as e:
            self.logger.warning(f"SCD Type 2 error: {e} - proceeding with append")
            return new_df

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

    def write_to_bronze(self, df: DataFrame, config: Dict) -> int:
        """Write DataFrame to bronze Delta table"""
        table_name = config.get('table_name')
        target_config = config.get('target', {})
        target_db = target_config.get('database')
        target_table = target_config.get('table')
        target_path = target_config.get('path')
        partition_cols = config.get('keys', {}).get('partition_columns', [])
        full_table_name = f"{target_db}.{target_table}"

        self.logger.info(f"Writing to: {full_table_name}")

        # Create database if needed
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_db}")

        # Add partition columns
        if partition_cols:
            for part_col in partition_cols:
                if part_col not in df.columns:
                    if part_col == 'year':
                        df = df.withColumn('year', year(col('_ingestion_timestamp')))
                    elif part_col == 'month':
                        df = df.withColumn('month', month(col('_ingestion_timestamp')))
                    elif part_col == 'day':
                        df = df.withColumn('day', dayofmonth(col('_ingestion_timestamp')))

        # Write to Delta
        writer = df.write.format('delta').mode('append')

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        if target_path:
            writer = writer.option('path', target_path)

        writer.saveAsTable(full_table_name)

        record_count = df.count()
        self.logger.info(f"Wrote {record_count} records to {full_table_name}")
        return record_count

    def ingest_table(self, table_name: str) -> Dict:
        """Main ingestion workflow"""
        self.logger.info("=" * 70)
        self.logger.info(f"STARTING INGESTION: {table_name}")
        self.logger.info("=" * 70)

        start_time = datetime.now()

        try:
            # Step 1: Load metadata
            config = self.load_metadata(table_name)

            if not config.get('enabled', True):
                self.logger.warning(f"Table {table_name} is disabled")
                return {
                    'table': table_name,
                    'status': 'SKIPPED',
                    'reason': 'Disabled in metadata',
                    'duration_seconds': 0
                }

            # Step 2: Load source data
            source_df = self.load_source_data(config)
            if source_df is None or source_df.count() == 0:
                self.logger.warning(f"No source data for {table_name}")
                return {
                    'table': table_name,
                    'status': 'SKIPPED',
                    'reason': 'No source data',
                    'duration_seconds': (datetime.now() - start_time).total_seconds()
                }

            # Step 3: Cast columns to metadata types FIRST
            self.logger.info("--- BEFORE CASTING ---")
            source_df.printSchema()
            source_df.show(10, False)

            source_df = self.cast_columns_to_metadata_types(source_df, config)

            self.logger.info("--- AFTER CASTING ---")
            source_df.printSchema()
            source_df.show(10, False)

            # Step 4: Apply data quality AFTER casting (so date validations work)
            source_df, dq_stats = self.apply_data_quality(source_df, config)

            # Step 5: Add audit columns
            df_with_audit = self.add_audit_columns(source_df, config)

            # Step 6: Apply SCD Type 2
            if config.get('scd', {}).get('enabled', False):
                final_df = self.apply_scd_type2(df_with_audit, config)
            else:
                final_df = df_with_audit

            # Step 7: Write to bronze
            records_written = self.write_to_bronze(final_df, config)

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