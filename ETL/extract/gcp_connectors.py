from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

project_id = os.getenv("project_id")
key_path = os.getenv("key_path")
bucket_suffix = os.getenv("bucket_suffix")

bucket_name = f"{project_id}-{bucket_suffix}"

credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
bq_client = bigquery.Client(credentials=credentials)
gcs_client = storage.Client(credentials=credentials)

def extract_from_bq(query):
    query_job = bq_client.query_and_wait(query)
    df = query_job.to_dataframe()
    return df

def upload_df_to_gcs(df, destination_blob_name, date):
    """Uploads a Pandas DataFrame to a GCS bucket as a CSV file."""
    year, month, day = date.split("-")
    # Convert DataFrame to CSV string
    csv_string = df.to_csv(index=False) 

    # Initialize GCS client
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(f"raw/{year}/{month}/{day}/{destination_blob_name}")

    # Upload the CSV string to GCS
    blob.upload_from_string(csv_string, content_type='text/csv')
