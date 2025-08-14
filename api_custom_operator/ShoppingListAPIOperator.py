"""
Simple custom Airflow operator for fetching shopping list data and uploading to GCS
"""

import json
import requests
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.exceptions import AirflowException


class ShoppingListAPIOperator(BaseOperator):
    """
    Fetches shopping list from API and uploads to GCS.

    Key concepts demonstrated:
    - Custom operator inheritance
    - API integration
    - GCS upload
    - Error handling
    """

    template_fields = ("gcs_object_name",)

    def __init__(self, api_url, gcs_bucket, gcs_object_name, **kwargs):
        super().__init__(**kwargs)
        self.api_url = api_url
        self.gcs_bucket = gcs_bucket
        self.gcs_object_name = gcs_object_name

    def execute(self, context):
        # Fetch data from API
        response = requests.get(self.api_url, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Upload to GCS
        gcs_hook = GCSHook()
        gcs_hook.upload(
            bucket_name=self.gcs_bucket,
            object_name=self.gcs_object_name,
            data=json.dumps(data).encode("utf-8"),
            mime_type="application/json",
        )

        # Return info for downstream tasks
        return {
            "items_count": len(data.get("items", [])),
            "gcs_path": f"gs://{self.gcs_bucket}/{self.gcs_object_name}",
        }
