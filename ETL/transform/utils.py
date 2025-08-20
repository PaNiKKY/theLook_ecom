from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, FloatType
from pyspark.sql import SparkSession

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

# Read csv data from GCS bucket
def read_csv_from_gcs(spark:SparkSession, 
                      file_path: str, 
                      schema:StructType,
                      bucket_name: str, 
                      year: str, 
                      month: str, 
                      day: str,
                      file_type: str = "csv"):
    if file_type == "parquet":
        df = spark.read.parquet(
            f"gs://{bucket_name}/clean/{year}/{month}/{day}/{file_path}.{file_type}",
            schema=schema,
            header=True
        )
    elif file_type == "csv":
        df = spark.read.csv(
            f"gs://{bucket_name}/raw/{year}/{month}/{day}/{file_path}.{file_type}",
            header=True,
            schema=schema
    )
    return df

# Save DataFrame to Parquet format
def save_to_parquet(df, 
                    file_name, 
                    bucket_name, 
                    year, 
                    month, 
                    day,
                    zone="clean"):
    df.write.mode("overwrite").parquet(f"gs://{bucket_name}/{zone}/{year}/{month}/{day}/{file_name}.parquet")