from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, FloatType
from pyspark.sql.functions import col, to_timestamp, lit, concat_ws, expr

import argparse
import json

parser = argparse.ArgumentParser()
parser.add_argument("--date_input", type=str)
parser.add_argument("--bucket_name", type=str)
parser.add_argument("--schema", type=str)  # Expecting a JSON string for the schema

args = parser.parse_args()

bucket_name = args.bucket_name
date = args.date_input
year, month, day = date.split("-")

schemas = json.loads(args.schema)

type_map = {
    "STRING": StringType(),
    "INTEGER": IntegerType(),
    "TIMESTAMP": TimestampType(),
    "FLOAT": FloatType(),
}

def create_schema(schema_list):
    """
    Create a Spark schema from a dictionary.
    """
    fields = []
    for field in schema_list:
        field_type = type_map[field["type"].upper()]
        nullable = field["mode"] == "NULLABLE"
        fields.append(StructField(field["name"], field_type, nullable))
    return StructType(fields)


spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Read csv data from GCS bucket
def read_csv_from_gcs(file_path, schema):
    df = spark.read.csv(
        f"gs://{bucket_name}/raw/{year}/{month}/{day}/{file_path}",
        header=True,
        schema=schema
    )
    return df

# Save DataFrame to Parquet format
def save_to_parquet(df, file_name):
    df.write.mode("overwrite").parquet(f"gs://{bucket_name}/clean/{year}/{month}/{day}/{file_name}.parquet")

# Process each table
for table_name, schema_list in schemas.items():
    # Get the schema for the current table
    schema = create_schema(schema_list)
    df = read_csv_from_gcs(f"{table_name}.csv", schema)

    # Clean and transform the DataFrame
    if table_name == "orders":
        df_order = df.select("order_id","created_at") \
               .withColumn("created_at", to_timestamp(col("created_at"))) \
               .withColumn("order_id", col("order_id").cast(IntegerType()))
        
        df_order.printSchema()
        df_order.show(3)
    
    elif table_name == "order_items":
        df = df.drop("id", "inventory_item_id", "created_at") \
                .withColumn("shipped_at", to_timestamp(col("shipped_at"))) \
                .withColumn("delivered_at", to_timestamp(col("delivered_at"))) \
                .withColumn("returned_at", to_timestamp(col("returned_at"))) \
                .withColumn("sale_price", col("sale_price").cast(FloatType())) \
                .withColumn("order_id", col("order_id").cast(IntegerType())) \
                .withColumn("user_id", col("user_id").cast(IntegerType())) \
                .withColumn("product_id", col("product_id").cast(IntegerType())) \
                .withColumn("status", col("status").cast(StringType())) \
                .groupBy("order_id", "product_id").count().alias("item_quantity") \
                .withColumnRenamed("count", "item_quantity") \
                .withColumn("item_quantity", col("item_quantity").cast(IntegerType())) \
        
        df = df.join(df_order, "order_id", "left") \
                .drop(df_order.order_id)
        
    elif table_name == "products":
        df = df.withColumn("cost", col("cost").cast(FloatType())) \
               .withColumn("retail_price", col("retail_price").cast(FloatType())) \
               .withColumn("distribution_center_id", col("distribution_center_id").cast(IntegerType())) \
               .withColumn("id", col("id").cast(IntegerType())) \
                .withColumn("category", col("category").cast(StringType())) \
                .withColumn("name", col("name").cast(StringType())) \
                .withColumn("brand", col("brand").cast(StringType())) \
                .withColumn("department", col("department").cast(StringType())) \
                .withColumn("sku", col("sku").cast(StringType())) \
                .dropDuplicates(["id"])

    elif table_name == "users":
        df = df.withColumn("created_at", to_timestamp(col("created_at"))) \
               .withColumn("user_geom", concat_ws(",", col("latitude"), col("longitude")))

    elif table_name == "distribution_centers":
        df = df.withColumn("distribution_center_geom", concat_ws(",", col("latitude"), col("longitude")))

    # Save the cleaned DataFrame to Parquet
    if table_name != "orders":
        df.printSchema()
        df.show(3)
        save_to_parquet(df, table_name)
