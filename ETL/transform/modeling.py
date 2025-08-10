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

spark = SparkSession.builder.appName("DataModeling").getOrCreate()

