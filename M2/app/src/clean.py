import pandas as pd
import os
from src.init_cleaning import init_cleaning
from src.handling_outliers import handling_outliers, handling_int_rate_outliers
from src.handling_missing import handle_missing
from src.handling_inconsistency import handle_inconsistencies
from src.transformation import transform, transform_grade
from src.db import save_to_db, add_rows_to_db


"""
The clean module for the transformation pipeline which includes the following functions:
- drop_extra_columns
- load_data
- save_data
- main : A function to handle the main transformation pipeline from loading the data to handling outliers, missing values, inconsistencies, and transformations
"""


DATASET_PATH = "./data/fintech_data_17_52_4509.csv"
CLEANED_DATA_PATH = "./data/fintech_data_MET_P1_52_4509_clean.csv"
LOOKUP_DF_PATH = "./data/lookup_table_MET_P1_52_4509.csv"
CLEANED_DATA_DB_TABLE = "fintech_data_MET_P1_52_4509_clean"
LOOKUP_TABLE_DB_TABLE = "lookup_table_MET_P1_52_4509"
STATES_DICT_PATH = "./data/usa_state_name_code_map.json"
EMP_LENGTH_MODEL_PATH = "./models/emp_length_model.pkl"
STREAMED_RAW_DATA_PATH = "./data/streamed_raw_data.csv"


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


def load_data(path: str = DATASET_PATH) -> pd.DataFrame:
    """A function to load a dataset from a CSV file
    Args:
        DATASET_PATH: A string representing the path to the dataset
    Returns:
        A pandas DataFrame
    """
    return pd.read_csv(path)


def save_original_data(df: pd.DataFrame, lookup_df: pd.DataFrame) -> None:
    """A function to save the cleaned data and the lookup table each as a CSV file
    Args:
        df: A pandas DataFrame
        lookup_df: A pandas DataFrame
    """
    print("Saving cleaned data to csv")
    df.to_csv(CLEANED_DATA_PATH)
    lookup_df.to_csv(LOOKUP_DF_PATH, index=False)


def main() -> None:
    """A function to handle the main transformation pipeline
    -load data
    - create lookup table
    - intial cleaning
    - handle inconsistencies
    - handle outliers
    - transform grade
    - handle missing values
    - handle int_rate outliers
    - transform
    - drop extra columns
    - save data to csvs
    - save data to database
    if the data already exists, it will load the data and the lookup table
    , skip the transformation pipeline and save the data to the database
    """

    if os.path.exists(CLEANED_DATA_PATH) and os.path.exists(LOOKUP_DF_PATH):
        print("Data already exists")
        df = load_data(CLEANED_DATA_PATH)
        lookup_df = load_data(LOOKUP_DF_PATH)
    else:
        print("Loading raw data")
        df = load_data(DATASET_PATH)
        print("Creating lookup table")
        lookup_df = pd.DataFrame(
            columns=["column", "original", "imputed", "impute_type"]
        )
        print("Starting transformation pipeline")
        df = init_cleaning(df)
        print("Handling inconsistencies")
        df, lookup_df = handle_inconsistencies(df, lookup_df, update_lookup=True)
        print("Handling outliers")
        df = handling_outliers(df)
        df, lookup_df = transform_grade(df, lookup_df, update_lookup=True)
        print("Handling missing values")
        df, lookup_df = handle_missing(
            df, lookup_df, EMP_LENGTH_MODEL_PATH, update_lookup=True
        )
        df = handling_int_rate_outliers(df)
        print("Transforming data")
        df, lookup_df = transform(df, lookup_df, STATES_DICT_PATH, update_lookup=True)
        df = drop_extra_columns(df)

        lookup_df = lookup_df.astype(str)
        save_original_data(df, lookup_df)

    print("Saving cleaned data to database")
    save_to_db(df, CLEANED_DATA_DB_TABLE)
    print("Saving lookup table to database")
    save_to_db(lookup_df, LOOKUP_TABLE_DB_TABLE)


def streamed_main(df: pd.DataFrame) -> None:
    """A function to handle the main transformation pipeline for streamed data
    -load data
    - create lookup table
    - intial cleaning
    - handle inconsistencies
    - handle outliers
    - transform grade
    - handle missing values
    - handle int_rate outliers
    - transform
    - drop extra columns
    - save data to csvs
    - save data to database
    if the data already exists, it will load the data and the lookup table
    , skip the transformation pipeline and save the data to the database

    Args:
        df: A pandas DataFrame representing the data to be cleaned and transformed
    """

    # if os.path.exists(STREAMED_RAW_DATA_PATH):
    #     streamed_raw_data = load_data(STREAMED_RAW_DATA_PATH)
    #     df = df.dropna(axis="index", how='all')
    #     streamed_raw_data = pd.concat([streamed_raw_data, df], ignore_index=True)
    #     streamed_raw_data = streamed_raw_data.drop_duplicates()
    # else:
    #     streamed_raw_data = pd.DataFrame(columns=df.columns)
    # streamed_raw_data.to_csv(STREAMED_RAW_DATA_PATH, index=False)

    dummy_lookup_df = pd.DataFrame(
        columns=["column", "original", "imputed", "impute_type"]
    )

    print("Starting transformation pipeline")
    df = init_cleaning(df)
    print("Handling inconsistencies")
    df = handle_inconsistencies(df, dummy_lookup_df)
    print("Handling outliers")
    df = handling_outliers(df)
    df = transform_grade(df, dummy_lookup_df)
    print("Handling missing values")
    df = handle_missing(df, dummy_lookup_df, EMP_LENGTH_MODEL_PATH)
    df = handling_int_rate_outliers(df)
    print("Transforming data")
    df = transform(df, dummy_lookup_df, STATES_DICT_PATH)
    df = drop_extra_columns(df)

    print("Saving cleaned streamed data to database")
    add_rows_to_db(df, CLEANED_DATA_DB_TABLE)
