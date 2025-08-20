from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, FloatType
from pyspark.sql.functions import col, to_timestamp, lit, concat_ws, expr, to_date

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


spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Process each table
for table_name, schema_list in schemas.items():
    # Get the schema for the current table
    schema = create_schema(schema_list)
    df = read_csv_from_gcs(spark=spark,
                            file_path=table_name,
                            schema=schema,
                            bucket_name=bucket_name,
                            year=year,
                            month=month,
                            day=day)

    # Clean and transform the DataFrame
    if table_name == "orders":
        df_order = df.select("order_id","created_at") \
               .withColumn("created_at",to_date(col("created_at"), "yyyy-MM-dd HH:mm:ss.SSSSSS z")) \
               .withColumn("order_id", col("order_id").cast(IntegerType())) \
               .dropDuplicates(["order_id"]) \
               .dropna(subset=["order_id", "created_at"])
        
        df_order.printSchema()
        df_order.show(3)
    
    elif table_name == "order_items":
        df = df.drop("id", "inventory_item_id", "created_at") \
                .withColumn("shipped_at", to_date(col("shipped_at"), "yyyy-MM-dd HH:mm:ss.SSSSSS z")) \
                .withColumn("delivered_at", to_date(col("delivered_at"), "yyyy-MM-dd HH:mm:ss.SSSSSS z")) \
                .withColumn("returned_at", to_date(col("returned_at"), "yyyy-MM-dd HH:mm:ss.SSSSSS z")) \
                .withColumn("sale_price", col("sale_price").cast(FloatType())) \
                .withColumn("order_id", col("order_id").cast(IntegerType())) \
                .withColumn("user_id", col("user_id").cast(IntegerType())) \
                .withColumn("product_id", col("product_id").cast(IntegerType())) \
                .withColumn("status", col("status").cast(StringType())) \
                .dropDuplicates(["order_id", "product_id"]) \
                .dropna(subset=["order_id", "product_id"])
        
        df_items = df.groupBy("order_id", "product_id").count().alias("item_quantity") \
                .withColumnRenamed("count", "item_quantity") \
                .withColumn("item_quantity", col("item_quantity").cast(IntegerType())) \
        
        df = df.join(df_order, "order_id", "left") \
                .join(df_items, ["order_id", "product_id"], "left") \
                .drop(df_order.order_id) \

        df.where(col("item_quantity") > 1).show(3)
        print(f"grouped by order_id and product_id: {df_items.count()}")
        print(f"total order_items: {df.count()}")
        
    elif table_name == "products":
        df = df.drop("retail_price") \
            .withColumn("cost", col("cost").cast(FloatType())) \
            .withColumn("distribution_center_id", col("distribution_center_id").cast(IntegerType())) \
            .withColumn("id", col("id").cast(IntegerType())) \
            .withColumn("category", col("category").cast(StringType())) \
            .withColumn("name", col("name").cast(StringType())) \
            .withColumn("brand", col("brand").cast(StringType())) \
            .withColumn("department", col("department").cast(StringType())) \
            .withColumn("sku", col("sku").cast(StringType())) \
            .withColumnRenamed("id", "product_id") \
            .dropDuplicates(["product_id"]) \
            .dropna(subset=["product_id"])

    elif table_name == "users":
        df = df.withColumnRenamed("id", "user_id") \
            .withColumn("user_id", col("user_id").cast(IntegerType())) \
            .withColumn("email", col("email").cast(StringType())) \
            .withColumn("first_name", col("first_name").cast(StringType())) \
            .withColumn("last_name", col("last_name").cast(StringType())) \
            .withColumn("username", concat_ws(" ", col("first_name"), col("last_name"))) \
            .withColumn("username", col("username").cast(StringType())) \
            .drop("first_name", "last_name", "user_geom") \
            .withColumn("age", col("age").cast(IntegerType())) \
            .withColumn("gender", col("gender").cast(StringType())) \
            .withColumn("country", col("country").cast(StringType())) \
            .withColumn("state", col("state").cast(StringType())) \
            .withColumn("city", col("city").cast(StringType())) \
            .withColumn("street_address", col("street_address").cast(StringType())) \
            .withColumn("postal_code", col("postal_code").cast(StringType())) \
            .withColumn("traffic_source", col("traffic_source").cast(StringType())) \
            .withColumn("latitude", col("latitude").cast(FloatType())) \
            .withColumn("longitude", col("longitude").cast(FloatType())) \
            .dropDuplicates(["user_id"]) \
            .dropna(subset=["user_id"])
               

    elif table_name == "distribution_centers":
        df = df.drop("distribution_center_geom") \
        .withColumn("id", col("id").cast(IntegerType())) \
        .withColumnRenamed("id", "distribution_center_id") \
        .withColumn("name", col("name").cast(StringType())) \
        .withColumn("latitude", col("latitude").cast(FloatType())) \
        .withColumn("longitude", col("longitude").cast(FloatType())) \
        .dropDuplicates(["distribution_center_id"]) \
        .dropna(subset=["distribution_center_id"])
        

    # Save the cleaned DataFrame to Parquet
    if table_name != "orders":
        df.printSchema()
        df.show(3)
        save_to_parquet(df=df,
                        file_name=table_name,
                        bucket_name=bucket_name,
                        year=year,
                        month=month,
                        day=day)
