"""
Simple BigQuery to GCS Export DAG
A simplified version that exports a BigQuery table to GCS in a single format
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
)

# Default arguments for the DAG
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Configuration
PROJECT_ID = "project"
DATASET_ID = "dataset"
TABLE_ID = "table"
GCS_BUCKET = "bucket"

# DAG Definition
dag = DAG(
    "simple_bq_to_gcs_export",
    default_args=default_args,
    description="Simple BigQuery to GCS export",
    schedule_interval="@daily",
    catchup=False,
    tags=["export", "bigquery", "gcs", "simple"],
)

# Check if table has data
check_data = BigQueryCheckOperator(
    task_id="check_table_data",
    sql=f"""
    SELECT COUNT(*) as row_count
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    HAVING COUNT(*) > 0
    """,
    use_legacy_sql=False,
    dag=dag,
)


# Export to GCS using BigQuery extract job
extract_job_config = {
    "extract": {
        "sourceTable": {
            "projectId": PROJECT_ID,
            "datasetId": DATASET_ID,
            "tableId": TABLE_ID,
        },
        "destinationUris": [
            f"gs://{GCS_BUCKET}/exports/{{{{ ds }}}}/data-*.csv"
        ],
        "destinationFormat": "CSV",
        "fieldDelimiter": ",",
        "printHeader": True,
    }
}

export_to_gcs = BigQueryInsertJobOperator(
    task_id="export_bq_to_gcs",
    configuration=extract_job_config,
    location="US",  # Set your BigQuery location
    dag=dag,
)

# Set task dependencies
check_data >> export_to_gcs
