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
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import json
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import sys
import os
import json

# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ETL.extract.gcp_connectors import load_df_from_gcs_to_bq, read_parquet_from_gcs, upload_df_to_gcs
from ETL.extract.extraction import get_df_from_bq
from src.great_expectation.gx import run_great_expectations
load_dotenv()

with open("/home/airflow/gcs/dags/src/schemas/bronze_schemas.json", "r") as f:
    bronze_schemas_dict = json.load(f)

with open("/home/airflow/gcs/dags/src/schemas/silver_schemas.json", "r") as f:
    silver_schemas_dict = json.load(f)

with open("/home/airflow/gcs/dags/src/schemas/gold_schemas.json", "r") as f:
    gold_schemas_dict = json.load(f)

with open("/home/airflow/gcs/dags/ETL/load/upsert.sql", "r") as f:
    upsert_query = f.read()


project_id = os.getenv("project_id")
# key_path = os.getenv("key_path")
bucket_suffix = os.getenv("bucket_suffix")
region = os.getenv("region")


utils_spark_path = f"gs://{project_id}-{bucket_suffix}-scripts/dags/ETL/transform/utils.py"
model_spark_path = f"gs://{project_id}-{bucket_suffix}-scripts/dags/ETL/transform/modeling.py"
clean_spark_path = f"gs://{project_id}-{bucket_suffix}-scripts/dags/ETL/transform/cleaning.py"

bucket_name = f"{project_id}-{bucket_suffix}"
cluster_name = f"{project_id}-cluster"

bucket_name = f"{project_id}-{bucket_suffix}"
bq_dataset = os.getenv("bq_dataset")

@dag(
    schedule_interval='@daily',
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['gcp', 'etl'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5)
    }
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
                                        "--schema", json.dumps(bronze_schemas_dict)
                                    ],
                            "python_file_uris": [
                                    utils_spark_path
                                ]
                            },
            }

    MODEL_JOB = {
            "reference": {"project_id": project_id},
            "placement": {"cluster_name": cluster_name},
            "pyspark_job": {"main_python_file_uri": model_spark_path,
                            "args": [
                                        "--date_input", date,
                                        "--bucket_name", bucket_name,
                                        "--schema", json.dumps(silver_schemas_dict)
                                    ],
                            "python_file_uris": [
                                    utils_spark_path
                                ]
                            },
            }
        
    
    @task_group(group_id="quality_check")
    def check_columns_quality():
        table_list = bronze_schemas_dict.keys()
        for table in table_list:
            @task(task_id=f"validate_columns_{table}_bq")
            def validate_bq_query(table):
                df = get_df_from_bq(
                    table_name=table,
                    filter=f"LIMIT 5"
                    )
                run_great_expectations(df, table)
            
            validate_bq_query(table)

    @task_group(group_id="extract")
    def extract_ecom_table_from_bq_to_gcs(): 
        @task
        def extract_order_table(date):
            df = get_df_from_bq(
                table_name="orders",
                # filter=f"WHERE DATE(created_at) = '{date}'"
                )
            
            print(df.head(3))
            upload_df_to_gcs(df, "orders.csv",bucket_name, date)
            return df["order_id"].tolist()
        @task
        def extract_order_items_table(order_ids,date):
            df = get_df_from_bq(
                table_name="order_items",
                filter=f"WHERE order_id IN {tuple(order_ids)}"
                )
            print(df.head(3))
            upload_df_to_gcs(df, "order_items.csv", bucket_name, date)


        @task
        def extract_product_table(date):
            df = get_df_from_bq(
                table_name="products"
                )
            print(df.head(3))
            upload_df_to_gcs(df, "products.csv",bucket_name, date)

        @task
        def extract_user_table(date):
            df = get_df_from_bq(
                table_name="users"
                )
            print(df.head(3))
            upload_df_to_gcs(df, "users.csv", bucket_name, date)

        @task
        def extract_distribution_table(date):
            df = get_df_from_bq(
                table_name="distribution_centers"
                )
            print(df.head(3))
            upload_df_to_gcs(df, "distribution_centers.csv",bucket_name, date)

        extract_order_items_table(extract_order_table(date), date)
        extract_product_table(date)
        extract_user_table(date)
        extract_distribution_table(date)

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=project_id,
        cluster_config=cluster_generator_config,
        region=region,
        cluster_name=cluster_name,
        # gcp_conn_id="gcp_conn"
    )


    @task_group(group_id="transform")
    def transform_ecom():
        cleaning_job_task = DataprocSubmitJobOperator(
            task_id="transform_job_task", 
            job=CLEAN_JOB,
            region=region, 
            project_id=project_id,
            # gcp_conn_id="gcp_conn"
        )

        modeling_job_task = DataprocSubmitJobOperator(
            task_id="modeling_job_task", 
            job=MODEL_JOB,
            region=region, 
            project_id=project_id,
            # gcp_conn_id="gcp_conn"
        )

        cleaning_job_task >> modeling_job_task

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=project_id,
        cluster_name=cluster_name,
        region=region,
        trigger_rule="none_failed_min_one_success",
        # gcp_conn_id="gcp_conn"
    )

    @task_group(group_id="check_quality_before_load")
    def check_quality_before_load():
        
        for table in gold_schemas_dict:
            table_name = table["table_name"]
            @task(task_id=f"validate_before_load_{table_name}")
            def validate_before_load(table, date):
                dataframe_to_validate=read_parquet_from_gcs(
                        table_name=table,
                        bucket_name=bucket_name,
                        date = date
                        )
                run_great_expectations(dataframe_to_validate, table)

            validate_before_load(table["table_name"], date)
    
    @task_group(group_id="load_to_parquet_bq")
    def load_parquet_to_bq():
        for table in gold_schemas_dict:
            table_name = table["table_name"]
            @task(task_id=f"load_{table_name}_to_bq")
            def load_to_bq(table, date):
                load_df_from_gcs_to_bq(
                        table_name=table,
                        bq_dataset=bq_dataset,
                        bucket_name=bucket_name,
                        date = date
                    )
            load_to_bq(table, date)
    
    upsert_to_dw_tables = BigQueryInsertJobOperator(
        task_id='upsert_to_dw_tables',
        configuration={
                "query": {
                    "query":upsert_query.format(
                        project_id=project_id,
                        bigquery_dataset=bq_dataset,
                        partition_date=">= '2015-01-01'"
                    ),
                    "useLegacySql": False,
                }
            },
        # gcp_conn_id="gcp_conn"
        )
    
    @task_group(group_id="delete_staging_tables")
    def delete_staging_tables():
        for table in gold_schemas_dict:
            table_name = table["table_name"]
            delete_staging_table = BigQueryInsertJobOperator(
                task_id=f'delete_staging_{table_name}',
                configuration={
                        "query": {
                            "query":
                                f"""
                                    DROP TABLE IF EXISTS `{project_id}.{bq_dataset}.staging_{table_name}`;
                                """,
                            "useLegacySql": False,
                        }
                    },
                # gcp_conn_id="gcp_conn"
                )
            delete_staging_table

    [check_columns_quality() >> extract_ecom_table_from_bq_to_gcs(), create_cluster] >> transform_ecom() >> check_quality_before_load() >> load_parquet_to_bq() >> upsert_to_dw_tables >> delete_staging_tables() >> delete_cluster

ecom_pipeline_dag()
