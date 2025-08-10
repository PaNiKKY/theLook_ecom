from airflow.decorators import dag, task, task_group
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import asyncio
import json
import os
from dotenv import load_dotenv
from datetime import datetime
import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ETL.extract.gcp_connectors import upload_df_to_gcs
from ETL.extract.extraction import get_df_from_bq, validate_df_columns
load_dotenv()

with open("ETL/schemas.json", "r") as f:
    schemas_dict = json.load(f)


project_id = os.getenv("project_id")
key_path = os.getenv("key_path")
bucket_suffix = os.getenv("bucket_suffix")
region = os.getenv("region")
clean_spark_file = os.getenv("clean_spark_file")
clean_spark_path = f"gs://{project_id}-{bucket_suffix}-scripts{clean_spark_file}"

bucket_name = f"{project_id}-{bucket_suffix}"
cluster_name = f"{project_id}-cluster"

bucket_name = f"{project_id}-{bucket_suffix}"

@dag(
    schedule_interval='@daily',
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['gcp', 'etl'],
)
def ecom_pipeline_dag():
    # date = "2025-06-21"
    date = "{{ ds }}"

    cluster_generator_config = ClusterGenerator(
        project_id=project_id,
        region=region,
        cluster_name=cluster_name,
        num_masters=1,
        master_machine_type='e2-standard-2',
        master_disk_size=100,
        num_workers=4,
        worker_machine_type='e2-standard-4',
        worker_disk_size=200,
        image_version='2.2-debian12',
        subnetwork_uri='default',
        internal_ip_only=True
    ).make()

    CLEAN_JOB = {
            "reference": {"project_id": project_id},
            "placement": {"cluster_name": cluster_name},
            "pyspark_job": {"main_python_file_uri": clean_spark_path,
                            "args": [
                                        "--date_input", date,
                                        "--bucket_name", bucket_name,
                                        "--schema", json.dumps(schemas_dict)
                                    ]
                            },
            }

    create_bucket_task = GCSCreateBucketOperator(
        task_id='create_bucket_if_not_exists',
        bucket_name=bucket_name,
        project_id=project_id,
        location=region.split("-")[0].upper(),
        gcp_conn_id="gcp_conn"
        )
    @task_group(group_id="extract")
    def extract_ecom_table_from_bq_to_gcs(): 
        @task
        def extract_order_table_validation(date):
            df = get_df_from_bq(
                table_name="orders",
                filter=f"WHERE DATE(created_at) = '{date}'"
                )
            
            print(df.head(3))
            is_pass = validate_df_columns(df, schemas_dict["orders"])
            if is_pass:
                upload_df_to_gcs(df, "orders.csv", date)
                return df["order_id"].tolist()
            else:
                raise ValueError("Validation failed for orders table has columns that do not match the expected schema.")

        @task
        def extract_order_items_table_validation(order_ids,date):
            df = get_df_from_bq(
                table_name="order_items",
                filter=f"WHERE order_id IN {tuple(order_ids)}"
                )
            print(df.head(3))
            is_pass = validate_df_columns(df, schemas_dict["order_items"])
            if is_pass:
                upload_df_to_gcs(df, "order_items.csv", date)
            else:
                raise ValueError("Validation failed for order_items table has columns that do not match the expected schema.")

        @task
        def extract_product_table_validation(date):
            df = get_df_from_bq(
                table_name="products"
                )
            print(df.head(3))
            is_pass = validate_df_columns(df, schemas_dict["products"])
            if is_pass:
                upload_df_to_gcs(df, "products.csv", date)
            else:
                raise ValueError("Validation failed for products table has columns that do not match the expected schema.")

        @task
        def extract_user_table_validation(date):
            df = get_df_from_bq(
                table_name="users"
                )
            print(df.head(3))
            is_pass = validate_df_columns(df, schemas_dict["users"])
            if is_pass:
                upload_df_to_gcs(df, "users.csv", date)
            else:
                raise ValueError("Validation failed for users table has columns that do not match the expected schema.")

        @task
        def extract_distribution_table_validation(date):
            df = get_df_from_bq(
                table_name="distribution_centers"
                )
            print(df.head(3))
            is_pass = validate_df_columns(df, schemas_dict["distribution_centers"])
            if is_pass:
                upload_df_to_gcs(df, "distribution_centers.csv", date)
            else:
                raise ValueError("Validation failed for distribution_centers table has columns that do not match the expected schema.")

        extract_order_items_table_validation(extract_order_table_validation(date), date)
        extract_product_table_validation(date)
        extract_user_table_validation(date)
        extract_distribution_table_validation(date)

    
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=project_id,
        cluster_config=cluster_generator_config,
        region=region,
        cluster_name=cluster_name,
        gcp_conn_id="gcp_conn"
    )

    transform_job_task = DataprocSubmitJobOperator(
        task_id="transform_job_task", 
        job=CLEAN_JOB,
        region=region, 
        project_id=project_id,
        gcp_conn_id="gcp_conn"
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=project_id,
        cluster_name=cluster_name,
        region=region,
        trigger_rule="none_failed_min_one_success",
        gcp_conn_id="gcp_conn"
    )

    create_bucket_task >> [extract_ecom_table_from_bq_to_gcs(), create_cluster] >> transform_job_task >> delete_cluster


ecom_pipeline_dag()
