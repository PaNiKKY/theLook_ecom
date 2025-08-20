import io
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

    buffer = io.BytesIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    # Initialize GCS client
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(f"raw/{year}/{month}/{day}/{destination_blob_name}")

    # Upload the CSV string to GCS
    blob.upload_from_file(buffer, content_type='text/csv', timeout=600)

def read_parquet_from_gcs(table_name, bucket_name, date) -> pd.DataFrame:
    
    year, month, day = date.split("-")
    bucket = gcs_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=f"modeling/{year}/{month}/{day}/{table_name}.parquet/")
    dfs = []
    for blob in blobs:
        if blob.name.endswith('.parquet'):
            # Download the blob content
            blob_content = blob.download_as_bytes()
            # Read Parquet content into a DataFrame
            df = pd.read_parquet(io.BytesIO(blob_content), engine='pyarrow')
            dfs.append(df)

    # Concatenate all DataFrames into a single DataFrame
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df
    else:
        return None
    
def load_df_from_gcs_to_bq(table_name, bq_dataset, schema, bucket_name, date) -> None:
 
    table_ref = bq_client.dataset(bq_dataset).table(f"staging_{table_name}")

    year, month, day = date.split("-")

    # Define the GCS URI of your source file
    gcs_uri = f"gs://{bucket_name}/modeling/{year}/{month}/{day}/{table_name}.parquet/*"  # or .json, etc.

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,  # or JSON, PARQUET, etc.
        autodetect=True,      # Automatically detect schema, or provide a schema
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # or WRITE_APPEND, WRITE_EMPTY
        schema=schema
    )

    # Start the load job
    load_job = bq_client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config,
    )

    # Wait for the job to complete
    load_job.result()

    print(f"Loaded {load_job.output_rows} rows into staging_{table_name}")