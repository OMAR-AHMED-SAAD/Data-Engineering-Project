#  outliers,transform_grade, handle_missing, hande int _rate_outliers

import pandas as pd
from Src.init_cleaning import init_cleaning
from Src.handling_outliers import handling_outliers, handling_int_rate_outliers
from Src.handling_missing import handle_missing
from Src.handling_inconsistency import handle_inconsistencies
from Src.transformation import transform, transform_grade

"""
The main module for the transformation pipeline which includes the following functions:
- drop_extra_columns
- load_data
- main : A function to handle the main transformation pipeline from loading the data to handling outliers, missing values, inconsistencies, and transformations
"""


DATASET_PATH = "./Data/fintech_data_17_52_4509.csv"
STATES_DICT_PATH = "./Data/usa_state_name_code_map.json"
EMP_LENGTH_MODEL_PATH = "./Models/emp_length_model.pkl"


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
        "addr_state_enc",
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

def main () -> None:
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
    """
    df = load_data(DATASET_PATH)
    lookup_df = pd.DataFrame(columns=['column', 'original', 'imputed', 'impute_type'])

    df = init_cleaning(df)
    df, lookup_df = handle_inconsistencies(df, lookup_df)
    df = handling_outliers(df)
    df, lookup_df = transform_grade(df, lookup_df)
    df, lookup_df = handle_missing(df, lookup_df, EMP_LENGTH_MODEL_PATH)
    df = handling_int_rate_outliers(df)
    df,lookup_df = transform(df, lookup_df, STATES_DICT_PATH)
    df = drop_extra_columns(df)