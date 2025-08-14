"""
Simple DAG using custom shopping list operator
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from custom_operators.shopping_list_operator import ShoppingListAPIOperator

default_args = {
    "owner": "data-team",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "shopping_list_simple",
    default_args=default_args,
    description="Fetch shopping list and store in GCS",
    schedule_interval="@daily",
    catchup=False,
)

# Main task: fetch API data and upload to GCS
fetch_data = ShoppingListAPIOperator(
    task_id="fetch_shopping_list",
    api_url="https://host/get-list",
    gcs_bucket="bucket",
    gcs_object_name="shopping-lists/{{ ds }}/data_{{ run_id }}.json",  # Templated filename
    dag=dag,
)


# Simple validation task
def validate_results(**context):
    result = context["task_instance"].xcom_pull(task_ids="fetch_shopping_list")
    items_count = result["items_count"]

    if items_count == 0:
        raise ValueError("No items found in shopping list")

    print(f"✅ Successfully processed {items_count} items")
    print(f"📁 Stored at: {result['gcs_path']}")
    return result


validate = PythonOperator(
    task_id="validate_data", python_callable=validate_results, dag=dag
)

# Set dependencies
fetch_data >> validate
