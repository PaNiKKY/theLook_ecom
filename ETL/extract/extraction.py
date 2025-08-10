import pandas as pd
from dotenv import load_dotenv
import os
import sys
import json

from ETL.extract.gcp_connectors import extract_from_bq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

dataset = os.getenv("source_dataset")

def get_df_from_bq(table_name, filter="" ) -> pd.DataFrame:
    query = f"""
                select * 
                from `{dataset}.{table_name}`
                {filter}
            """
    df = extract_from_bq(query)
    return df

def validate_df_columns(df: pd.DataFrame, expected_columns: list) -> bool:

    expected_columns = [col["name"] for col in expected_columns]
    df_columns = df.columns.tolist()
    try:
        assert sorted(df_columns) == sorted(expected_columns), "DataFrame columns do not match expected columns."
        return True
    except AssertionError as e:
        print(f"Validation Error: {e}")
        return False


