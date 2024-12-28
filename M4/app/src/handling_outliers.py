import pandas as pd
import numpy as np
import json

OUTLIERS_CAPS_PATH = "/opt/airflow/data/outliers_caps.json"

"""
A module for handling outliers in a DataFrame

Functions:
    cap_outliers_IQR: A function to cap the outliers in a column using the IQR method
    cap_outliers_IQR_int_rate: A function to cap the outliers in the int_rate column using the IQR method
    transform_then_cap_outliers: A function to handle outliers in a column by taking the log transformation and then capping the outliers
    handling_outliers: A function to handle outliers in a DataFrame except for the int_rate column
    handling_int_rate_outliers: A function to handle outliers in the int_rate column with a custom method
"""


def cap_outliers_IQR(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """A function to cap the outliers in a column using the IQR method
    Args:
        df: A pandas DataFrame
        column: A string
    Returns:
        A pandas DataFrame
    """
    try:
        with open(OUTLIERS_CAPS_PATH, "r") as f:
            outliers_caps = json.load(f)
    except FileNotFoundError:
        outliers_caps = {}

    if column not in outliers_caps:
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        outliers_caps[column] = {"lower_bound": lower_bound, "upper_bound": upper_bound}

        with open(OUTLIERS_CAPS_PATH, "w") as f:
            json.dump(outliers_caps, f, indent=4)
    else:
        lower_bound = outliers_caps[column]["lower_bound"]
        upper_bound = outliers_caps[column]["upper_bound"]

    df[column] = np.where(df[column] < lower_bound, lower_bound, df[column])
    df[column] = np.where(df[column] > upper_bound, upper_bound, df[column])
    return df


def cap_outliers_IQR_int_rate(
    df: pd.DataFrame, column: str, grade: str
) -> pd.DataFrame:
    """A function to cap the outliers in a column using the IQR method for the int_rate column
    Args:
        df: A pandas DataFrame
        column: A string
    Returns:
        A pandas DataFrame
    """
    try:
        with open(OUTLIERS_CAPS_PATH, "r") as f:
            outliers_caps = json.load(f)
    except FileNotFoundError:
        outliers_caps = {}

    if column not in outliers_caps or grade not in outliers_caps[column]:
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        outliers_caps[column] = {
            **outliers_caps.get(column, {}),
            grade: {"lower_bound": lower_bound, "upper_bound": upper_bound},
        }

        with open(OUTLIERS_CAPS_PATH, "w") as f:
            json.dump(outliers_caps, f, indent=4)
    else:
        lower_bound = outliers_caps[column][grade]["lower_bound"]
        upper_bound = outliers_caps[column][grade]["upper_bound"]

    df[column] = np.where(df[column] < lower_bound, lower_bound, df[column])
    df[column] = np.where(df[column] > upper_bound, upper_bound, df[column])
    return df


def transform_then_cap_outliers(
    df: pd.DataFrame,
    column: str,
    transformation: np.ufunc,
    transformation_name: str,
    cap: bool = True,
) -> pd.DataFrame:
    """A function to handle outliers in a column by taking the log transformation and then capping the outliers
    Args:
        df: A pandas DataFrame
        column: A string
    Returns:
        A pandas DataFrame
    """
    new_column = column + transformation_name
    df[new_column] = transformation(df[column])
    df = cap_outliers_IQR(df, new_column) if cap else df
    return df


def handling_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """A function to handle outliers in a DataFrame
    for columns annual_inc, annual_inc_joint, avg_cur_bal,tot_cur_bal,tot_cur_bal,funded_amount and int_rate
    Args:
        df: A pandas DataFrame
    Returns:
        A pandas DataFrame
    """

    log_then_cap_columns = [
        "annual_inc",
        "annual_inc_joint",
        "avg_cur_bal",
        "tot_cur_bal",
    ]
    sqrt = ["loan_amount", "funded_amount"]
    for column in log_then_cap_columns:
        df = transform_then_cap_outliers(df, column, np.log1p, "_log", cap=True)

    for column in sqrt:
        df = transform_then_cap_outliers(df, column, np.sqrt, "_sqrt", cap=False)

    return df


def handling_int_rate_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    A function to handle outliers in the int_rate column after grouping by grade and capping the outliers

    Args:
        df: A pandas DataFrame

    Returns:
        A pandas DataFrame
    """
    df["int_rate_outliers_capped"] = (
        df.groupby("grade")
        .apply(lambda x: cap_outliers_IQR_int_rate(x, "int_rate", x.name))
        .reset_index(0, drop=True)["int_rate"]
    )
    return df