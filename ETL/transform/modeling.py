from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, FloatType, DateType
from pyspark.sql.functions import col, to_timestamp, lit, concat_ws, expr, md5, date_format

import argparse
import json

from utils import create_schema, read_csv_from_gcs, save_to_parquet

parser = argparse.ArgumentParser()
parser.add_argument("--date_input", type=str)
parser.add_argument("--bucket_name", type=str)
parser.add_argument("--schema", type=str)  # Expecting a JSON string for the schema

args = parser.parse_args()

bucket_name = args.bucket_name
date = args.date_input
year, month, day = date.split("-")

schemas = json.loads(args.schema)

# Create schema for each table
schemas = {table_name: create_schema(schema_list) for table_name, schema_list in schemas.items()}

spark = SparkSession.builder.appName("DataModeling").getOrCreate()

# Read parquet files from the clean directory
order_items_df = read_csv_from_gcs(spark=spark,
                                    file_path="order_items",
                                    schema=schemas["order_items"],
                                    bucket_name=bucket_name,
                                    year=year,
                                    month=month,
                                    day=day,
                                    file_type="parquet")

products_df = read_csv_from_gcs(spark=spark,
                                 file_path="products",
                                    schema=schemas["products"],
                                    bucket_name=bucket_name,
                                    year=year,
                                    month=month,
                                    day=day,
                                    file_type="parquet")

users_df = read_csv_from_gcs(spark=spark,
                             file_path="users",
                             schema=schemas["users"],
                             bucket_name=bucket_name,
                             year=year, 
                             month=month,
                             day=day,
                             file_type="parquet")

distribution_centers_df = read_csv_from_gcs(spark=spark,
                                            file_path="distribution_centers",
                                            schema=schemas["distribution_centers"],
                                            bucket_name=bucket_name,
                                            year=year,
                                            month=month,
                                            day=day,
                                            file_type="parquet")


# Craete hash key for order_items with MD5 by using all columns
products_df = products_df.withColumn("product_key",
                                            md5(concat_ws(",",  
                                                          *(col(c).cast("string")
                                                            for c in products_df.columns)
                                                        )
                                                )
                                            )

users_df = users_df.withColumn("user_key",
                                            md5(concat_ws(",",  
                                                          *(col(c).cast("string")
                                                            for c in users_df.columns)
                                                        )
                                                )
                                            )

distribution_centers_df = distribution_centers_df.withColumn("distribution_center_key",
                                            md5(concat_ws(",",  
                                                          *(col(c).cast("string")                                    
                                                            for c in distribution_centers_df.columns)
                                                        )
                                                )
                                            )

order_items_df = order_items_df.join(products_df.select("product_id", "product_key", "cost", "distribution_center_id"), "product_id", "left") \
                                .join(users_df.select("user_id", "user_key"), "user_id", "left") \
                                .join(distribution_centers_df.select("distribution_center_id", "distribution_center_key"), "distribution_center_id", "left") \
                                .drop("product_id", "user_id", "distribution_center_id") \
                                .withColumn("created_at_partition", col("created_at").cast(DateType())) \
                                .withColumn("created_at", date_format(col("created_at"), "yyyyMMdd").cast(IntegerType())) \
                                .withColumn("shipped_at", date_format(col("shipped_at"), "yyyyMMdd").cast(IntegerType())) \
                                .withColumn("delivered_at", date_format(col("delivered_at"), "yyyyMMdd").cast(IntegerType())) \
                                .withColumn("returned_at", date_format(col("returned_at"), "yyyyMMdd").cast(IntegerType())) \
                                .withColumn("total_cost", col("cost") * col("item_quantity")) \
                                .withColumn("total_sale_price", col("sale_price") * col("item_quantity")) \
                                .withColumn("profit", col("total_sale_price") - col("total_cost")) \
                                

products_df = products_df.drop("cost", "distribution_center_id")

order_items_df.printSchema()
order_items_df.show(3)

products_df.printSchema()
products_df.show(3)

users_df.printSchema()
users_df.show(3)

distribution_centers_df.printSchema()
distribution_centers_df.show(3)

save_to_parquet(df=order_items_df,
                file_name="fact_order_items",
                bucket_name=bucket_name,
                year=year,
                month=month,
                day=day,
                zone="modeling")

save_to_parquet(df=products_df,
                file_name="dim_products",
                bucket_name=bucket_name,
                year=year,
                month=month,                
                day=day,
                zone="modeling")
save_to_parquet(df=users_df,
                file_name="dim_users",
                bucket_name=bucket_name,                
                year=year,
                month=month,
                day=day,
                zone="modeling")
save_to_parquet(df=distribution_centers_df,
                file_name="dim_distributions",
                bucket_name=bucket_name,                
                year=year,
                month=month,
                day=day,
                zone="modeling")