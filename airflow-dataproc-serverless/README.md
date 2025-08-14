# Customer Analytics ETL DAG - Setup Guide

This guide walks you through setting up and testing the Customer Analytics ETL DAG in Google Cloud Composer. The DAG processes daily customer transaction data using Dataproc Serverless (Spark) and validates data quality in BigQuery.

## 1. Set Environment Variables

Set up key environment variables that will be used throughout this guide:

```bash
# Core project configuration
export PROJECT_ID="your-actual-project-id"
export GCS_BUCKET="your-actual-data-bucket"
export BQ_DATASET="analytics"
export COMPOSER_ENV="composer-demo"
export COMPOSER_LOCATION="us-central1"

# Service account configuration
export DATAPROC_SA="dataproc-service-account"

# Network configuration
export NETWORK_NAME="dataproc-network"
export SUBNET_NAME="dataproc-subnet"
export SUBNET_RANGE="10.0.0.0/16"
export REGION="us-central1"

# Test data configuration  
export TEST_DATE="2024-01-15"

# Verify variables are set
echo "Project ID: $PROJECT_ID"
echo "GCS Bucket: $GCS_BUCKET"
echo "BigQuery Dataset: $BQ_DATASET"
```

## 2. Fix DAG Configuration

### Update Project Configuration
Replace placeholder values with your environment variables in your DAG file:

```python
PROJECT_ID = your-actual-project-id
GCS_BUCKET = your-actual-bucket
```

## 3. Create GCS Bucket Structure

Set up your GCS bucket with the following directory structure:

```
gs://your-actual-bucket/
├── scripts/
│   ├── customer_etl.py      # Main Spark ETL script
│   └── utils.py             # Utility functions
├── raw/
│   └── transactions/
│       └── YYYY-MM-DD/      # Date partitioned raw data
│           └── transactions.parquet
└── processed/
    └── customer_analytics/
        └── YYYY-MM-DD/      # Date partitioned processed data
```

Create the bucket and initial structure:

```bash
# Create bucket (if it doesn't exist)
gsutil mb gs://$GCS_BUCKET

# Create directory structure
gsutil -m cp /dev/null gs://$GCS_BUCKET/scripts/.keep
gsutil -m cp /dev/null gs://$GCS_BUCKET/raw/transactions/.keep
gsutil -m cp /dev/null gs://$GCS_BUCKET/processed/customer_analytics/.keep
```

## 4. Create Spark ETL Script

Upload the script:

```bash
gsutil cp customer_etl.py gs://$GCS_BUCKET/scripts/
```

## 5. Configure IAM and Networking

### Service Account Setup

Create a dedicated service account for Dataproc jobs:

```bash
# Create the service account
gcloud iam service-accounts create $DATAPROC_SA \
    --description="Service account for Dataproc Serverless jobs" \
    --display-name="Dataproc Service Account"

# Assign required roles
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$DATAPROC_SA@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/dataproc.worker"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$DATAPROC_SA@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$DATAPROC_SA@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"
```

### Configure Composer Service Account

Grant the Composer environment permissions to trigger Dataproc jobs:

```bash
# Get Composer service account
COMPOSER_SA=$(gcloud composer environments describe $COMPOSER_ENV \
    --location=$COMPOSER_LOCATION \
    --format="value(config.nodeConfig.serviceAccount)")

# Grant permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$COMPOSER_SA" \
    --role="roles/dataproc.editor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$COMPOSER_SA" \
    --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$COMPOSER_SA" \
    --role="roles/storage.objectViewer"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$COMPOSER_SA" \
    --role="roles/iam.serviceAccountUser"
```

### Network Configuration

Create VPC network and subnet for Dataproc:

```bash
# Create VPC network
gcloud compute networks create $NETWORK_NAME \
    --subnet-mode=custom \
    --description="Network for Dataproc jobs"

# Create subnet
gcloud compute networks subnets create $SUBNET_NAME \
    --network=$NETWORK_NAME \
    --range=$SUBNET_RANGE \
    --region=$REGION \
    --enable-private-ip-google-access \
    --description="Subnet for Dataproc Serverless jobs"

# Configure firewall rules
gcloud compute firewall-rules create dataproc-internal \
    --network=$NETWORK_NAME \
    --allow=tcp,udp,icmp \
    --source-ranges=$SUBNET_RANGE \
    --description="Allow internal communication in dataproc network"
```

## 6. Generate Test Data

Use the test data generation script (see separate artifact) to create sample transaction data:

```bash
python generate_test_data.py --date $TEST_DATE --output gs://$GCS_BUCKET/raw/transactions/$TEST_DATE/transactions.parquet
```

## 7. Deploy the DAG

Upload your DAG file to the Composer environment:

```bash
# Using gcloud CLI
gcloud composer environments storage dags import \
    --environment $COMPOSER_ENV \
    --location $COMPOSER_LOCATION \
    --source customer_analytics_dag.py
```

Or use the Cloud Console:
1. Navigate to **Cloud Composer** → Your Environment
2. Click **Environment details** → **DAGs folder**
3. Upload `customer_analytics_dag.py`

### Verify DAG Parsing

1. Go to **Cloud Composer** in Google Cloud Console
2. Click on your Composer environment
3. Click **Open Airflow UI**
4. Check that `customer_analytics_dag` appears in the DAGs list
5. Verify there are no parsing errors (red indicators)

## 8. Testing the DAG

### Manual DAG Testing

1. In Airflow UI, find your DAG
2. Toggle it ON (if not already enabled)
3. Click the "Trigger DAG" button (play icon)
4. Set execution date (e.g., $TEST_DATE)
5. Monitor task progress in the Graph View

### Validate Results

```bash
# Check processed data exists in GCS
gsutil ls gs://$GCS_BUCKET/processed/customer_analytics/$TEST_DATE/

# Verify BigQuery data
bq query --use_legacy_sql=false \
"SELECT * FROM \`$PROJECT_ID.$BQ_DATASET.customer_daily_summary\` 
 WHERE date = '$TEST_DATE' LIMIT 10"

# Check record count
bq query --use_legacy_sql=false \
"SELECT COUNT(*) as record_count 
 FROM \`$PROJECT_ID.$BQ_DATASET.customer_daily_summary\` 
 WHERE date = '$TEST_DATE'"
```