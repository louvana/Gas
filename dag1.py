from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='hello_world',
    description='A simple test DAG',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # Define a simple task
    hello_task = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello from Airflow! DAG loaded successfully."'
    )   
