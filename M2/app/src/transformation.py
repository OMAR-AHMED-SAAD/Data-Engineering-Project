import pandas as pd
from typing import List, Tuple
from sklearn.preprocessing import MinMaxScaler
import json
import pickle as pkl
import os

"""
A file for transformation functions which includes three parts:
- Part 1: Functions to add features to the DataFrame
    - add_features
- Part 2: Functions to encode columns in the DataFrame
    - add_one_hot_encoding
    - add_label_encoding
    - update_lookup_table_one_hot
    - update_lookup_table_label
    - encode_and_update
    - encode_columns
- Part 3: Functions to normalize columns in the DataFrame
    - min_max_scale_column
    - normlize_columns
- transform: A function to transform the data using all the 3 parts except for the grade column which is handled separately elsewhere
"""


# ---------------- Part 1 ----------------


def add_features(df: pd.DataFrame, states_dict_path: str) -> pd.DataFrame:
    """A function to add features to the DataFrame
    - add a column for the month number
    - add a column to indicate if the salary can cover the loan amount
    - add a column for the monthly installment
    - add a column for the state name
    Args:
        df: A pandas DataFrame
    Returns:
        A pandas DataFrame
    """
    df["issue_date"] = pd.to_datetime(df["issue_date"])
    df["month_number"] = df["issue_date"].dt.month

    df["salary_can_cover"] = (df["annual_inc_joint"] > df["loan_amount"]).astype(int)

    P = df["funded_amount"]
    r = df["int_rate_outliers_capped"] / 12
    n = df["term"]

    df["installment_per_month"] = P * r * (1 + r) ** n / ((1 + r) ** n - 1)

    with open(states_dict_path, "rb") as f:
        states_dict = json.load(f)

    df["state_name"] = df["state"].map(states_dict)
    return df


# ---------------- Part 2 ----------------


def add_one_hot_encoding(
    df: pd.DataFrame, column: str
) -> Tuple[pd.DataFrame, List[str]]:
    """A function to add one-hot encoding to a column in a DataFrame
    Args:
        df: A pandas DataFrame
        column: A string
    Returns:
        A pandas DataFrame, List[str]
    """
    encoding_file = f"data/encodings/{column}_enc.json"
    df[column] = df[column].astype(str).str.lower().str.replace(" ", "_")
    try:
        with open(encoding_file, "r") as f:
            old_values = json.load(f)[column]
    except FileNotFoundError:
        old_values = df[column].unique()
        with open(encoding_file, "w") as f:
            json.dump({column: old_values.tolist()}, f)

    dummies = pd.get_dummies(df[column], prefix=column)
    dummies = dummies.astype(int)

    for value in old_values:
        if column + "_" + value not in dummies.columns:
            dummies[column + "_" + value] = 0
    old_values = [column + "_" + value for value in old_values]
    dummies = dummies[old_values]

    df = pd.concat([df, dummies], axis=1)
    return df, old_values


def add_label_encoding(
    df: pd.DataFrame, column: str
) -> Tuple[pd.DataFrame, List[str], List[int]]:
    """A function to add label encoding to a column in a DataFrame
    Args:
        df: A pandas DataFrame
        column: A string
    Returns:
        A pandas DataFrame, List[str], List[int]
    """
    encoding_file = f"data/encodings/{column}_enc.json"

    if os.path.exists(encoding_file):
        with open(encoding_file, "r") as f:
            encoding_dict = json.load(f)
        df[column + "_enc"] = df[column].astype(str).map(encoding_dict)
        return df, [], []
    else:
        unique_sorted = sorted(df[column].unique())
        old_values = unique_sorted

        df[column + "_enc"] = pd.Categorical(
            df[column], categories=unique_sorted, ordered=True
        ).codes
        new_values = sorted(df[column + "_enc"].unique())

        with open(encoding_file, "w") as f:
            encoding_dict = {
                str(key): int(value) for key, value in zip(unique_sorted, new_values)
            }
            json.dump(encoding_dict, f)

        return df, old_values, new_values


def update_lookup_table_one_hot(
    lookup_df: pd.DataFrame, column: str, old_values: List[str]
) -> pd.DataFrame:
    """A function to update the lookup table with one-hot encoding
    Args:
        lookup_df: A pandas DataFrame
        column: A string
    Returns:
        A pandas DataFrame
    """
    for old in old_values:
        new_row = pd.DataFrame(
            [
                {
                    "column": column,
                    "original": old,
                    "imputed": {old},
                    "impute_type": "one-hot-encoding",
                }
            ]
        )
        lookup_df = pd.concat([lookup_df, new_row], ignore_index=True)
    return lookup_df


def update_lookup_table_label(
    lookup_df: pd.DataFrame, column: str, old_values: List[str], new_values: List[int]
) -> pd.DataFrame:
    """A function to update the lookup table with label encoding
    Args:
        lookup_df: A pandas DataFrame
        column: A string
    Returns:
        A pandas DataFrame
    """
    for old, new in zip(old_values, new_values):
        new_row = pd.DataFrame(
            [
                {
                    "column": column,
                    "original": old,
                    "imputed": new,
                    "impute_type": "label-encoding",
                }
            ]
        )
        lookup_df = pd.concat([lookup_df, new_row], ignore_index=True)
    return lookup_df


def encode_and_update(
    df: pd.DataFrame,
    column: str,
    lookup_df: pd.DataFrame,
    encoding_type_threshold: int = 5,
    encode_type: str = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """A function to encode a column and update the lookup table based on the encoding type threshold
        Specify the encoding type if you want to force a specific encoding type
    Args:
        df: A pandas DataFrame
        column: A string
        lookup_df: A pandas DataFrame
        encoding_type_threshold: An integer
        encode_type: A string
    Returns:
        A pandas DataFrame,
        A pandas DataFrame,
    """
    encoding_file = f"data/encodings/{column}_enc.json"
    if os.path.exists(encoding_file):
        with open(encoding_file, "r") as f:
            encoding_dict = json.load(f)
        num_unique = len(encoding_dict)
    else :
        num_unique = df[column].nunique()
    if (
         num_unique< encoding_type_threshold
        and encode_type != "label-encoding"
    ):
        df, old_values = add_one_hot_encoding(df, column)
        lookup_df = update_lookup_table_one_hot(lookup_df, column, old_values)
    else:
        df, old_values, new_values = add_label_encoding(df, column)
        lookup_df = update_lookup_table_label(lookup_df, column, old_values, new_values)
    return df, lookup_df


def encode_columns(
    df: pd.DataFrame, lookup_df: pd.DataFrame, encoding_type_threshold: int = 5
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """A function to encode columns in a DataFrame
    Args:
        df: A pandas DataFrame
        lookup_df: A pandas DataFrame
        encoding_type_threshold: An integer
    Returns:
        A pandas DataFrame,
        A pandas DataFrame
    """
    categorical_columns = {
        "home_ownership": "any",
        "verification_status": "any",
        "purpose": "any",
        "grade": "label-encoding",
        "loan_status": "any",
        "type": "any",
        "state": "any",
        "addr_state": "any",
        "pymnt_plan": "label-encoding",
    }

    for column, encode_type in categorical_columns.items():
        df, lookup_df = encode_and_update(
            df, column, lookup_df, encode_type=encode_type
        )
    return df, lookup_df


def transform_grade(
    df: pd.DataFrame, lookup_df: pd.DataFrame, update_lookup: bool = False
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """A function to handle the grade column
    Args:
        df: A pandas DataFrame
        lookup_df: A pandas DataFrame
        update_lookup: A boolean to update the lookup table
    Returns:
        A tuple of 2 pandas DataFrames
    """

    def map_grade(value):
        grade_map = {5: "A", 10: "B", 15: "C", 20: "D", 25: "E", 30: "F", 35: "G"}
        for threshold, grade in grade_map.items():
            if value <= threshold:
                return grade
        return None

    df["grade"] = df["grade"].apply(map_grade)

    grade_map = {
        "1-5": "A",
        "6-10": "B",
        "11-15": "C",
        "16-20": "D",
        "21-25": "E",
        "26-30": "F",
        "31-35": "G",
    }
    for threshold, grade in grade_map.items():
        new_row = pd.DataFrame(
            [
                {
                    "column": "grade",
                    "original": threshold,
                    "imputed": grade,
                    "impute_type": "encoding",
                }
            ]
        )
        lookup_df = pd.concat([lookup_df, new_row], ignore_index=True)

    if not update_lookup:
        return df

    return df, lookup_df


# ---------------- Part 3 ----------------


def min_max_scale_column(
    df: pd.DataFrame, old_column: str, new_column: str
) -> pd.DataFrame:
    """A function to normalize a column in a DataFrame using min-max scaling in a new column
    Args:
        df: A pandas DataFrame
        column: A string
    Returns:
        A pandas DataFrame
    """

    if os.path.exists(f"data/scalers/{old_column}_scaler.pkl"):
        with open(f"data/scalers/{old_column}_scaler.pkl", "rb") as f:
            scaler = pkl.load(f)
        df[new_column] = scaler.transform(df[[old_column]])
    else:
        scaler = MinMaxScaler()
        df[new_column] = scaler.fit_transform(df[[old_column]])
        with open(f"data/scalers/{old_column}_scaler.pkl", "wb") as f:
            pkl.dump(scaler, f)
    return df


def normlize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """A function to normalize columns in a DataFrame using min-max scaling
    Args:
        df: A pandas DataFrame
    Returns:
        A pandas DataFrame
    """
    df = min_max_scale_column(df, "int_rate_outliers_capped", "int_rate_normalized")
    df = min_max_scale_column(df, "loan_amount_sqrt", "loan_amount_sqrt_normalized")
    df = min_max_scale_column(df, "funded_amount_sqrt", "funded_amount_sqrt_normalized")
    df = min_max_scale_column(
        df, "installment_per_month", "installment_per_month_normalized"
    )
    return df


# ---------------- transform function ----------------


def transform(
    df: pd.DataFrame,
    lookup_df: pd.DataFrame,
    states_dict_path: str,
    update_lookup: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """A function to transform the data
    Args:
        df: A pandas DataFrame
        lookup_df: A pandas DataFrame
        states_dict_path: A string representing the path to the states dictionary
        update_lookup: A boolean to update the lookup table
    Returns:
        A tuple of 2 pandas DataFrames
    """
    df = add_features(df, states_dict_path)
    df, lookup_df = encode_columns(df, lookup_df)
    df = normlize_columns(df)

    if not update_lookup:
        return df
    return df, lookup_df
