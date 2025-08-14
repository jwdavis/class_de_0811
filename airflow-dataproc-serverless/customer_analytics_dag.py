"""
Example 1: Daily ETL Pipeline - Customer Analytics
This DAG processes customer transaction data daily, transforming raw data
into analytics-ready format using serverless Spark.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
)

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)

from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator

# Default arguments for the DAG
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

schema_fields = [
    {"name": "customer_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "unique_transactions", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "distinct_categories", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "distinct_payment_methods", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "avg_transaction_amount", "type": "FLOAT", "mode": "NULLABLE"},
]

# DAG Definition
dag1 = DAG(
    "customer_analytics_etl",
    default_args=default_args,
    description="Daily customer transaction analytics ETL",
    catchup=False,
    tags=["etl", "analytics", "daily"],
)

# Configuration
PROJECT_ID = "project_id"
REGION = "us-central1"
GCS_BUCKET = "bucket_name"

# Serverless Spark job for ETL processing
customer_etl_batch = DataprocCreateBatchOperator(
    task_id="run_customer_etl",
    project_id=PROJECT_ID,
    region=REGION,
    batch={
        "pyspark_batch": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/scripts/customer_etl.py",
            "args": [
                "--input_path",
                f"gs://{GCS_BUCKET}/raw/transactions/{{{{ ds }}}}",
                "--output_path",
                f"gs://{GCS_BUCKET}/processed/customer_analytics/{{{{ ds }}}}",
                "--date",
                "{{ ds }}",
            ],
        },
        "environment_config": {
            "execution_config": {
                "service_account": (
                    f"dataproc-service-account@{PROJECT_ID}.iam.gserviceaccount.com"
                ),
                "subnetwork_uri": (
                    f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/dataproc-subnet"
                ),
            },
        },
        "runtime_config": {
            "version": "2.1",  # Spark version
        },
        "labels": {"job_type": "etl", "team": "analytics", "env": "prod"},
    },
    batch_id="customer-etl-{{ ds_nodash }}-{{ ts_nodash | replace('T', '-') }}",
    dag=dag1,
)

create_external_table = BigQueryInsertJobOperator(
    task_id="create_external_table",
    configuration={
        "query": {
            "query": (
                f"""
                CREATE OR REPLACE EXTERNAL TABLE analytics.customer_daily_summary OPTIONS(
                    format = 'PARQUET',
                    uris = ['gs://{GCS_BUCKET}/processed/customer_analytics/{{{{ ds }}}}/*.parquet']
                )
            """
            ),
            "useLegacySql": False,
        }
    },
    dag=dag1,
)

table_exists_sensor = BigQueryTableExistenceSensor(
    task_id="wait_for_table",
    project_id=PROJECT_ID,
    dataset_id="analytics",
    table_id="customer_daily_summary",
    dag=dag1,
)


# Modified Data quality check
quality_check = BigQueryCheckOperator(
    task_id="data_quality_check",
    sql=f"""
    SELECT COUNT(*) as record_count
    FROM `{PROJECT_ID}.analytics.customer_daily_summary`
    """,  # Removed WHERE clause
    use_legacy_sql=False,
    dag=dag1,
)

# Set dependencies
customer_etl_batch >> create_external_table >> table_exists_sensor >> quality_check
