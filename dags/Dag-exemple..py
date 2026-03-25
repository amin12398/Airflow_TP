from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow'
}

with DAG(
    dag_id='example_dag',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 6, 1),
    catchup=False
) as dag:

    task1 = BashOperator(
        task_id='task_1',
        bash_command='echo "Tche 1 execute"'
    )

    task2 = BashOperator(
        task_id='task_2',
        bash_command='echo "Tche 2 execute"'
    )

    task1 >> task2  # task1 s'exécute avant task2