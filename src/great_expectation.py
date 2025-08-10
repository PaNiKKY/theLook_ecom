import great_expectations as gx
import great_expectations.expectations as gxe
from pyspark.sql import SparkSession
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--date_input", type=str)
parser.add_argument("--bucket_name", type=str)

args = parser.parse_args()

bucket_name = args.bucket_name
date = args.date_input
year, month, day = date.split("-")

spark = SparkSession.builder.appName("Validating_Cleaned_Data").getOrCreate()

df = spark.read.parquet(f"s3://{bucket_name}/clean/{year}/{month}/{day}/")

context = gx.get_context()

data_source = context.data_sources.add_spark(name="my_spark_data_source")

data_asset = data_source.add_dataframe_asset(name="cleaned_data_asset")

batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "batch definition"
    )
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

expectation_suite = context.suites.add(
        gx.ExpectationSuite(name="customer expectations")
    )

expectations = [
    gxe.ExpectColumnValuesToNotBeNull(column="order_id")
]

for expectation in expectations:
    expectation_suite.add_expectation(expectation)

validation_result = batch.validate(expectation_suite)