import sys

sys.path.append('/opt/airflow/src')

from functions import extract_clean, transform
from db import load_to_db


from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


DATASET_PATH = "/opt/airflow/data/fintech_data_17_52_4509.csv"
CLEANED_INTERMEDIATE_DATA_PATH = "/opt/airflow/data/fintech_clean.csv"
TRANSFORMED_DATA_PATH = "/opt/airflow/data/fintech_transformed.csv"

# Define the DAG
default_args = {
    "owner": "Omar_Ahmed",
    "depends_on_past": False,
    'start_date': days_ago(2),
    "retries": 1,
}

dag = DAG(
    'fintech_pipeline',
    default_args=default_args,
    description='fintech etl pipeline and dashboard',
)

with DAG(
    dag_id = 'fintech_pipeline',
    schedule_interval = '@once', # could be @daily, @hourly, etc or a cron expression '* * * * *'
    default_args = default_args,
    tags = ['fintech-pipeline'],
)as dag:
    # Define the tasks
    extract_clean_task = PythonOperator(
        task_id = 'extract_clean',
        python_callable = extract_clean,
        op_kwargs = {
            'data_path': DATASET_PATH,
            'intermediate_data_path': CLEANED_INTERMEDIATE_DATA_PATH
        }
    )

    transform_task = PythonOperator(
        task_id = 'transform',
        python_callable = transform,
        op_kwargs = {
            'intermediate_data_path': CLEANED_INTERMEDIATE_DATA_PATH,
            'transformed_data_path': TRANSFORMED_DATA_PATH
        }
    )

    load_to_db_task = PythonOperator(
        task_id = 'load_to_db',
        python_callable = load_to_db,
        op_kwargs = {
            'transformed_data_path': TRANSFORMED_DATA_PATH
        }
    )

    # Define the task to run the Dash app
    run_dashboard = BashOperator(
        task_id='run_dashboard',
        bash_command='python /opt/airflow/src/fintech_dashbaord.py',  # Path to your Dash app script
    )

    # Define the task dependencies
    extract_clean_task >> transform_task >> load_to_db_task >> run_dashboard
