import pandas as pd
import great_expectations as gx
import json
from great_expectations.core import ExpectationSuite
from great_expectations.dataset import PandasDataset
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def run_great_expectations(df, table_name):

    # context = gx.get_context()

    ge_df = gx.from_pandas(df)

    # --------------------------
    # 2. Load expectation suite from JSON
    # --------------------------
    with open(f"src/great_expectation/expectations/{table_name}_expectation_suite.json") as f:  # replace with your JSON path
        suite_json = json.load(f)

    suite = ExpectationSuite(
        expectation_suite_name=suite_json["expectation_suite_name"],
        expectations=suite_json["expectations"]
    )

    # --------------------------
    # 3. Run validation
    # --------------------------
    results = ge_df.validate(expectation_suite=suite)

    # --------------------------
    # 4. Print summary
    # --------------------------
    print("Validation Results Summary:")
    print(results)

    # Optionally: raise an error if validation fails
    if not results["success"]:
        raise ValueError("Data validation failed!")
