from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['data-eng@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'payment_data_pipeline',
    default_args=default_args,
    description='Payment Pipeline - Bronze → Silver → Gold',
    schedule_interval='0 2 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['payment', 'medallion'],
    max_active_runs=1
)

bronze_task = DatabricksRunNowOperator(
    task_id='bronze_layer',
    databricks_conn_id='databricks_default',
    job_id=123,
    notebook_params={'environment': 'dev'},
    dag=dag
)

silver_task = DatabricksRunNowOperator(
    task_id='silver_layer',
    databricks_conn_id='databricks_default',
    job_id=124,
    notebook_params={'environment': 'dev'},
    dag=dag
)

gold_task = DatabricksRunNowOperator(
    task_id='gold_layer',
    databricks_conn_id='databricks_default',
    job_id=125,
    notebook_params={'environment': 'dev'},
    dag=dag
)

bronze_task >> silver_task >> gold_task