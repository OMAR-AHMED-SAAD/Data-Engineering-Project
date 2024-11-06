import pandas as pd
import numpy as np

"""
A module for handling inconsistency in data which includes the following functions:
- handle_emp_length
- handle_term
- handle_type
"""


def handle_emp_length(df: pd.DataFrame, lookup_df: pd.DataFrame) -> pd.DataFrame:
    """A function to handle the emp_length column and update the lookup table
    Args:
        df: A pandas DataFrame
        lookup_df: A pandas DataFrame
    Returns:
         A tuple of 2 pandas DataFrames
    """

#    convert nan to -1
    df["emp_length"] = df["emp_length"].fillna("-1")
    df["emp_length"] = df["emp_length"].str.replace("years", "").str.strip()
    df["emp_length"] = df["emp_length"].str.replace("year", "").str.strip()
    df["emp_length"] = df["emp_length"].str.replace("< 1", "0.5").str.strip()
    df["emp_length"] = df["emp_length"].str.replace("10+", "11").str.strip()
    df["emp_length"] = df["emp_length"].astype("float")
    df["emp_length"] = df["emp_length"].replace(-1, np.nan)
    new_row1 = pd.DataFrame(
        [
            {
                "column": "emp_length",
                "original": "< 1 years",
                "imputed": "0.5",
                "impute_type": "custom",
            }
        ]
    )
    new_row2 = pd.DataFrame(
        [
            {
                "column": "emp_length",
                "original": "10+ years",
                "imputed": "11",
                "impute_type": "custom",
            }
        ]
    )

    lookup_df = pd.concat([lookup_df, new_row1], ignore_index=True)
    lookup_df = pd.concat([lookup_df, new_row2], ignore_index=True)

    return df, lookup_df


def handle_term(df: pd.DataFrame, lookup_df: pd.DataFrame) -> pd.DataFrame:
    """A function to handle the term column and update the lookup table
    Args:
        df: A pandas DataFrame
        lookup_df: A pandas DataFrame
    Returns:
         A tuple of 2 pandas DataFrames
    """
    df["term"] = df["term"].str.replace("months", "").str.strip()
    df["term"] = df["term"].astype("int")
    new_row1 = pd.DataFrame(
        [
            {
                "column": "term",
                "original": "36 months",
                "imputed": "36",
                "impute_type": "custom",
            }
        ]
    )
    new_row2 = pd.DataFrame(
        [
            {
                "column": "term",
                "original": "60 months",
                "imputed": "60",
                "impute_type": "custom",
            }
        ]
    )

    lookup_df = pd.concat([lookup_df, new_row1], ignore_index=True)
    lookup_df = pd.concat([lookup_df, new_row2], ignore_index=True)
    return df, lookup_df


def handle_type(df: pd.DataFrame) -> pd.DataFrame:
    """A function to handle the loan_status column
    Args:
        df: A pandas DataFrame
    Returns:
        A pandas DataFrame
    """
    df["type"] = df["type"].str.lower()
    df["type"] = df["type"].str.replace(" ", "_")
    df["type"] = df["type"].str.replace("joint_app", "joint")
    return df


def handle_inconsistencies(
    df: pd.DataFrame, lookup_df: pd.DataFrame, update_lookup: bool = False
) -> pd.DataFrame:
    """A function to handle inconsistencies in the DataFrame
    Args:
        df: A pandas DataFrame
        lookup_df: A pandas DataFrame
    Returns:
        A pandas DataFrame
    """
    df, lookup_df = handle_emp_length(df, lookup_df)
    df, lookup_df = handle_term(df, lookup_df)
    df = handle_type(df)

    if not update_lookup:
        return df

    return df, lookup_df
