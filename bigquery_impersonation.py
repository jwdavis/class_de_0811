from google.cloud import bigquery
from google.auth import impersonated_credentials
from google.oauth2 import service_account
import google.auth
import os

def create_bigquery_client_with_impersonation():
    """
    Create a BigQuery client using service account impersonation
    This allows you to impersonate a service account without having its key file
    """
    
    # The service account email you want to impersonate
    target_service_account = "your-service-account@your-project.iam.gserviceaccount.com"
    
    # Define the scopes needed for BigQuery
    target_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    
    try:
        # Get default credentials (from ADC, metadata server, etc.)
        source_credentials, project_id = google.auth.default()
        
        # Create impersonated credentials
        impersonated_creds = impersonated_credentials.Credentials(
            source_credentials=source_credentials,
            target_principal=target_service_account,
            target_scopes=target_scopes,
            # Optional: delegates for chained impersonation
            # delegates=["intermediate-account@project.iam.gserviceaccount.com"]
        )
        
        # Initialize BigQuery client with impersonated credentials
        client = bigquery.Client(
            credentials=impersonated_creds,
            project=project_id  # or specify a different project
        )
        
        print(f"BigQuery client created with impersonated service account: {target_service_account}")
        print(f"Using project: {project_id}")
        
        return client
        
    except Exception as e:
        print(f"Failed to create impersonated credentials: {e}")
        return None

def create_client_with_source_service_account():
    """
    Alternative: Use a source service account key file to impersonate another service account
    This is useful when you have a service account that can impersonate others
    """
    
    # Source service account (the one doing the impersonation)
    source_service_account_path = "path/to/source-service-account-key.json"
    
    # Target service account (the one being impersonated)
    target_service_account = "target-service-account@your-project.iam.gserviceaccount.com"
    
    target_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    
    try:
        # Load source credentials from key file
        if os.path.exists(source_service_account_path):
            source_credentials = service_account.Credentials.from_service_account_file(
                source_service_account_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
        else:
            print(f"Source service account file not found: {source_service_account_path}")
            return None
        
        # Create impersonated credentials
        impersonated_creds = impersonated_credentials.Credentials(
            source_credentials=source_credentials,
            target_principal=target_service_account,
            target_scopes=target_scopes
        )
        
        # Initialize client
        client = bigquery.Client(
            credentials=impersonated_creds,
            project=source_credentials.project_id
        )
        
        print(f"Impersonation chain: {source_credentials.service_account_email} -> {target_service_account}")
        
        return client
        
    except Exception as e:
        print(f"Failed to create impersonated credentials with source SA: {e}")
        return None

def run_query_with_impersonation():
    """
    Run a BigQuery query using service account impersonation
    """
    
    # Create client with impersonation
    client = create_bigquery_client_with_impersonation()
    
    if not client:
        print("Failed to create BigQuery client with impersonation")
        return
    
    # Query using your partitioned table
    query = """
        SELECT 
            *
        FROM `project_id.utils.partitioned_log_data`
        ORDER BY _PARTITIONTIME DESC
        LIMIT 5
    """
    
    try:
        # Configure query job
        job_config = bigquery.QueryJobConfig(
            use_legacy_sql=False,
            dry_run=False,
            maximum_bytes_billed=10**9  # 1GB limit
        )
        
        print("Executing query with impersonated service account...")
        query_job = client.query(query, job_config=job_config)
        
        # Wait for job completion and get results
        results = query_job.result()
        
        print(f"Query completed successfully!")
        print(f"Job ID: {query_job.job_id}")
        print(f"Processed {query_job.total_bytes_processed:,} bytes")
        print(f"Found {results.total_rows:,} rows")
        
        # Display results
        print(f"\nLatest {min(5, results.total_rows)} rows:")
        print("-" * 60)
        
        for i, row in enumerate(results):
            print(f"Row {i+1}: {dict(row)}")
            
    except Exception as e:
        print(f"Query failed: {e}")
        print("Check that:")
        print("1. The impersonated service account has BigQuery permissions")
        print("2. Your source credentials can impersonate the target account")
        print("3. The table exists and you have access to it")

def test_impersonation_permissions():
    """
    Test impersonation by checking what datasets are accessible
    """
    client = create_bigquery_client_with_impersonation()
    
    if not client:
        return
    
    try:
        # Test by listing datasets
        print("Testing impersonated account permissions...")
        datasets = list(client.list_datasets())
        
        if datasets:
            print(f"Successfully accessed {len(datasets)} datasets:")
            for dataset in datasets[:5]:  # Show first 5
                print(f"  - {dataset.dataset_id}")
            if len(datasets) > 5:
                print(f"  ... and {len(datasets) - 5} more")
        else:
            print("No datasets found (this might be normal)")
            
        # Test getting current project info
        print(f"Current project: {client.project}")
        
    except Exception as e:
        print(f"Permission test failed: {e}")

def demonstrate_chained_impersonation():
    """
    Example of chained impersonation (A -> B -> C)
    Useful in complex environments with multiple service accounts
    """
    
    # Chain: Your credentials -> Intermediate SA -> Final SA
    intermediate_sa = "intermediate@your-project.iam.gserviceaccount.com"
    final_sa = "final-target@your-project.iam.gserviceaccount.com"
    
    target_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    
    try:
        # Get source credentials
        source_credentials, project_id = google.auth.default()
        
        # Create chained impersonation
        impersonated_creds = impersonated_credentials.Credentials(
            source_credentials=source_credentials,
            target_principal=final_sa,
            target_scopes=target_scopes,
            delegates=[intermediate_sa]  # Chain through this account
        )
        
        client = bigquery.Client(
            credentials=impersonated_creds,
            project=project_id
        )
        
        print(f"Chained impersonation: You -> {intermediate_sa} -> {final_sa}")
        return client
        
    except Exception as e:
        print(f"Chained impersonation failed: {e}")
        return None

if __name__ == "__main__":
    print("BigQuery with Service Account Impersonation")
    print("=" * 50)
    
    # Test impersonation setup
    print("1. Testing impersonation permissions...")
    test_impersonation_permissions()
    
    print("\n" + "=" * 50)
    
    # Run query with impersonation
    print("2. Running query with impersonated service account...")
    run_query_with_impersonation()
    
    print("\n" + "=" * 50)
    print("3. Example of chained impersonation (commented out):")
    print("   # demonstrate_chained_impersonation()")
