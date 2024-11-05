import pandas as pd

"""
A module for cleaning data which includes the following functions:
- rename_columns
- set_Index
- remove_duplicates
- init_cleaning : A function that uses the whole module to clean the data
"""

def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """A function to rename a df columns
    Args:
        df: A pandas DataFrame
    Returns:
        A pandas DataFrame
    Purpose:
        - remove leading and trailing whitespaces
        - replace spaces with underscores
        - convert to lowercase
    """
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df


def set_Index(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """A function to set a column as an index
    Args:
        df: A pandas DataFrame
        column: A string
    Returns:
        A pandas DataFrame
    """
    df = df.set_index(column)
    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """A function to remove duplicates from a DataFrame
    Args:
        df: A pandas DataFrame
    Returns:
        A pandas DataFrame
    """
    df = df.drop_duplicates()
    return df


def init_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """A function to clean the initial data
    Args:
        df: A pandas DataFrame
    Returns:
        A pandas DataFrame
    """
    df = rename_columns(df)
    df = set_Index(df, 'loan_id')
    df = remove_duplicates(df)
    return df