"""
Simple BigQuery Query with Email Notification DAG
Runs a BigQuery query using InsertJobOperator and sends email notification when complete
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# Default arguments for the DAG
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["admin@lab.com"],
}

# Configuration
PROJECT_ID = "project_id"  # Replace with your actual project ID
# TODO: set this to an existing GCS bucket you want to write timestamps to
GCS_BUCKET = "bucket_name"

# DAG Definition
dag = DAG(
    "simple_bq_query",
    default_args=default_args,
    description="Simple BigQuery query",
    schedule_interval="@daily",
    catchup=False,
    tags=["bigquery", "email", "simple"],
)

# BigQuery job configuration for a simple query
query_job_config = {
    "query": {
        "query": """
        SELECT
            c.cust_id,
            COUNT(o.order_num)
        FROM
            `roi-bq-demos.bq_demo.customer` c
        JOIN
            `roi-bq-demos.bq_demo.order` o
        ON
            o.cust_id = c.cust_id
        GROUP BY
            c.cust_id
        """,
        "useLegacySql": False,
        "useQueryCache": False,
        "writeDisposition": "WRITE_TRUNCATE"
    }
}

# Run BigQuery job
run_query = BigQueryInsertJobOperator(
    task_id="run_bq_query",
    configuration=query_job_config,
    location="US",  # Set your BigQuery location
    dag=dag,
)


def capture_start_time(**kwargs):
    """Return the current UTC time as ISO string (pushed to XCom)."""
    return datetime.utcnow().isoformat()


def write_times_to_gcs(**kwargs):
    """Pull start time from XCom, capture end time, and write both to GCS."""
    ti = kwargs["ti"]
    start_time = ti.xcom_pull(task_ids="capture_start_time")
    end_time = datetime.utcnow().isoformat()

    content = f"start: {start_time}\nend: {end_time}\n"

    # Use a temporary file and GCSHook.upload with filename for compatibility
    import tempfile
    import os

    tmp = tempfile.NamedTemporaryFile(delete=False, mode="w", encoding="utf-8")
    try:
        tmp.write(content)
        tmp.flush()
        tmp.close()

        # Object name includes DAG id and execution date to avoid collisions
        run_id = kwargs.get("run_id") or kwargs.get("dag_run").run_id if kwargs.get("dag_run") else "manual"
        object_name = f"{dag.dag_id}/timestamps_{run_id}.txt"

        gcs = GCSHook()
        # NOTE: ensure GCS_BUCKET exists and the Airflow connection has permissions
        gcs.upload(bucket_name=GCS_BUCKET, object_name=object_name, filename=tmp.name)
    finally:
        try:
            os.remove(tmp.name)
        except Exception:
            pass


capture_start = PythonOperator(
    task_id="capture_start_time",
    python_callable=capture_start_time,
    dag=dag,
)

write_timestamps = PythonOperator(
    task_id="write_times_to_gcs",
    python_callable=write_times_to_gcs,
    provide_context=True,
    dag=dag,
)

# Wire the tasks: capture start -> run query -> write timestamps
capture_start >> run_query >> write_timestamps

