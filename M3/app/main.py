import pandas as pd
from src.db import save_to_db 

FINTECH_CLEANED_PATH = "data/fintech_spark_52_4509.parquet"
LOOKUP_TABLE_PATH = "data/lookup_spark_52_4509.parquet"


if __name__ == "__main__":
    fintech_df = pd.read_parquet(FINTECH_CLEANED_PATH)
    lookup_table_df = pd.read_parquet(LOOKUP_TABLE_PATH)
    save_to_db(fintech_df, "fintech_df")
    save_to_db(lookup_table_df, "lookup_table")