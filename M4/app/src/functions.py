import pandas as pd
import os
from init_cleaning import init_cleaning
from handling_outliers import handling_outliers, handling_int_rate_outliers
from handling_missing import handle_missing
from handling_inconsistency import handle_inconsistencies
from transformation import transform_fn, transform_grade

"""
The clean module for the transformation pipeline which includes the following functions:
- drop_extra_columns
- load_data
- main : A function to handle the main transformation pipeline from loading the data to handling outliers, missing values, inconsistencies, and transformations
"""

STATES_DICT_PATH = "/opt/airflow/data/usa_state_name_code_map.json"
EMP_LENGTH_MODEL_PATH = "/opt/airflow/models/emp_length_model.pkl"
STREAMED_RAW_DATA_PATH = "/opt/airflow/data/streamed_raw_data.csv"


def drop_extra_columns(df: pd.DataFrame) -> pd.DataFrame:
    """A function to drop extra columns from a DataFrame
    Args:
        df: A pandas DataFrame

    Returns:
        A pandas DataFrame
    """

    columns_to_drop = [
        "emp_length",
        "annual_inc",
        "annual_inc_joint",
        "avg_cur_bal",
        "tot_cur_bal",
        "home_ownership",
        "verification_status",
        "purpose",
        "int_rate",
        "int_rate_outliers_capped",
        "state",
        "addr_state",
        "type",
        "loan_status",
        "pymnt_plan",
        "loan_amount",
        "funded_amount",
        "grade",
        "loan_amount_sqrt",
        "funded_amount_sqrt",
    ]
    df = df.drop(columns=columns_to_drop)

    return df


def load_data(path: str) -> pd.DataFrame:
    """A function to load a dataset from a CSV file
    Args:
        DATASET_PATH: A string representing the path to the dataset
    Returns:
        A pandas DataFrame
    """
    return pd.read_csv(path)


def extract_clean(data_path: str, intermediate_data_path: str) -> None:
    if os.path.exists(intermediate_data_path):
        print("Data already cleaned")
        df = load_data(intermediate_data_path)
    else:
        print("Loading raw data")
        df = load_data(data_path)
        lookup_df = pd.DataFrame(
            columns=["column", "original", "imputed", "impute_type"])
        print("Starting transformation pipeline")
        df = init_cleaning(df)
        print("Handling inconsistencies")
        df, lookup_df = handle_inconsistencies(df,
                                               lookup_df,
                                               update_lookup=True)
        print("Handling outliers")
        df = handling_outliers(df)
        df, lookup_df = transform_grade(df, lookup_df, update_lookup=True)
        print("Handling missing values")
        df, lookup_df = handle_missing(df,
                                       lookup_df,
                                       EMP_LENGTH_MODEL_PATH,
                                       update_lookup=True)
        df = handling_int_rate_outliers(df)
        print("Saving cleaned data")
        df.to_csv(intermediate_data_path)


def transform(intermediate_data_path: str, transformed_data_path: str) -> None:
    if os.path.exists(transformed_data_path):
        print("Data already Transformed")
        df = load_data(transformed_data_path)
    else:
        print("Loading raw data")
        df = load_data(intermediate_data_path)
        lookup_df = pd.DataFrame(
            columns=["column", "original", "imputed", "impute_type"])
        print("Transforming data")
        df, lookup_df = transform_fn(df,
                                     lookup_df,
                                     STATES_DICT_PATH,
                                     update_lookup=True)
        # df = drop_extra_columns(df)
        print("Saving transformed data")
        df.to_csv(transformed_data_path)
