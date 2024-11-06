import pandas as pd
from sklearn import linear_model
import pickle as pkl
import json
import os

MEANS_DICT_PATH = 'data/means_dict.json'

"""
A module for handling missing values in a DataFrame which includes the following functions:
- handle_annual_inc_joint
- handle_int_rate
- handle_description
- handle_emp_length
- handle_emp_title
- handle_missing : A function to handle missing values in a DataFrame using the above functions 
"""

def handle_annual_inc_joint(df: pd.DataFrame, lookup_df: pd.DataFrame) -> pd.DataFrame:
    """A function to handle the annual_inc_joint column and update the lookup table
    Args:
        df: A pandas DataFrame
        lookup_df: A pandas DataFrame
    Returns:
        A tuple of 2 pandas DataFrames
    """
    df["annual_inc_joint"] = df["annual_inc_joint"].fillna(df["annual_inc"])
    df['annual_inc_joint_log'] = df['annual_inc_joint_log'].fillna(df['annual_inc_log'])

    new_row = pd.DataFrame([{'column': 'annual_inc_joint', 'original': 'nan', 'imputed': 'annual_inc', 'impute_type': 'custom'}])
    lookup_df = pd.concat([lookup_df, new_row], ignore_index=True)

    return df, lookup_df


def handle_int_rate(df: pd.DataFrame) -> pd.DataFrame:
    """A function to handle the int_rate column and update the lookup table
    Args:
        df: A pandas DataFrame
    Returns:
        A pandas DataFrame
    """

    if os.path.exists(MEANS_DICT_PATH):
        with open(MEANS_DICT_PATH, 'r') as f:
            int_rate_data = json.load(f)['int_rate']
        df['int_rate'] = df.apply(lambda x: int_rate_data[x['state']][x['grade']] if pd.isnull(x['int_rate']) else x['int_rate'], axis=1)
    else:
        means_dict = df.groupby(['state', 'grade'])['int_rate'].mean().unstack(fill_value=0).to_dict(orient='index')
        df['int_rate'] = df.groupby(['state','grade'])['int_rate'].transform(lambda x: x.fillna(x.mean()))
        with open(MEANS_DICT_PATH, 'w') as f:
            json.dump({'int_rate':means_dict}, f, indent=4)
    return df

def handle_description(df: pd.DataFrame, lookup_df: pd.DataFrame) -> pd.DataFrame:
    """A function to handle the desc column
    Args:
        df: A pandas DataFrame
        lookup_df: A pandas DataFrame
    Returns:
        A tuple of 2 pandas DataFrames
    """
    df['description'] = df['description'].fillna('missing')
    new_row = pd.DataFrame([{'column': 'description', 'original': 'nan', 'imputed': 'missing', 'impute_type': 'custom'}])
    lookup_df = pd.concat([lookup_df, new_row], ignore_index=True)
    return df, lookup_df

def handle_emp_length(df: pd.DataFrame, model_path: str = 'emp_length_model.pkl') -> pd.DataFrame:
    """A function to handle the emp_length column
    Args:
        df: A pandas DataFrame
        model_path: A string representing the path to the model
    Returns:
        A pandas DataFrame
    """

    try:
        filename = model_path
        model = pkl.load(open(filename
                                , 'rb'))
    except FileNotFoundError:
        print('Model not found. Training a new model')
        model = linear_model.LinearRegression()
        training_df = df[df['emp_length'].notnull()]
        model.fit(training_df[['annual_inc_log', 'avg_cur_bal_log', 'tot_cur_bal_log']], training_df['emp_length'])
        pkl.dump(model, open(filename, 'wb'))
    
    df['emp_length_predictions'] = model.predict(df[['annual_inc_log', 'avg_cur_bal_log', 'tot_cur_bal_log']])
    df['emp_length_predictions'] = df['emp_length_predictions'].round()
    df['emp_length_imputed'] = df['emp_length'].fillna(df['emp_length_predictions'])
    df = df.drop(columns=['emp_length_predictions'])
    return df


def handle_emp_title(df: pd.DataFrame, lookup_df: pd.DataFrame) -> pd.DataFrame:
    """A function to handle the emp_title column
    Args:
        df: A pandas DataFrame
        lookup_df: A pandas DataFrame
    Returns:
        A tuple of 2 pandas DataFrames
    """
    df['emp_title'] = df['emp_title'].fillna('missing')
    new_row = pd.DataFrame([{'column': 'emp_title', 'original': 'nan', 'imputed': 'missing', 'impute_type': 'custom'}])
    lookup_df = pd.concat([lookup_df, new_row], ignore_index=True)
    return df, lookup_df

def handle_missing(df: pd.DataFrame, lookup_df: pd.DataFrame, model_path: str = 'emp_length_model.pkl', update_lookup: bool = False) -> pd.DataFrame:
    """A function to handle missing values in a DataFrame
    Args:
        df: A pandas DataFrame
        lookup_df: A pandas DataFrame
        model_path: A string representing the path to the model
        update_lookup: A boolean to update the lookup table
    Returns:
        A pandas DataFrame
    """
    df , lookup_df = handle_annual_inc_joint(df, lookup_df)
    df = handle_int_rate(df)
    df, lookup_df = handle_description(df, lookup_df)
    df = handle_emp_length(df, model_path)
    df , lookup_df= handle_emp_title(df, lookup_df)

    if not update_lookup:
        return df

    return df, lookup_df