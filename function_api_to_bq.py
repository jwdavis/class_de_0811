import functions_framework
import requests
import json
from google.cloud import bigquery
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@functions_framework.http
def streaming_inserts_function(request):
    """
    Cloud Function that fetches data from API and writes to BigQuery using streaming inserts
    """
    try:
        # Initialize BigQuery client
        client = bigquery.Client()

        # API endpoint
        api_url = "https://shopping-list-api-376236993953.us-central1.run.app/get-list"

        # Make API call
        logger.info(f"Making API call to {api_url}")
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()

        # Parse JSON response
        data = response.json()
        logger.info(f"Received data: {data}")

        # Prepare data for BigQuery insert
        # Add timestamp to the data
        row_data = {**data, "inserted_at": datetime.utcnow().isoformat()}

        # Define table reference
        table_id = "project_id.shopping_data.shopping_list_data"

        # Insert row using streaming inserts
        table = client.get_table(table_id)
        rows_to_insert = [row_data]

        logger.info(f"Inserting row into {table_id}")
        errors = client.insert_rows_json(table, rows_to_insert)

        if errors:
            logger.error(f"BigQuery insert errors: {errors}")
            return {
                "statusCode": 500,
                "body": json.dumps(
                    {"message": "Error inserting into BigQuery", "errors": errors}
                ),
            }

        logger.info("Successfully inserted data into BigQuery")
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Data successfully inserted using streaming inserts",
                    "data": row_data,
                }
            ),
        }

    except requests.RequestException as e:
        logger.error(f"API call failed: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "API call failed", "error": str(e)}),
        }

    except Exception as e:
        logger.error(f"Function execution failed: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps(
                {"message": "Function execution failed", "error": str(e)}
            ),
        }
