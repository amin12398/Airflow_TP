from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def extract_and_transform():
  
    data = pd.read_csv('/usr/local/airflow/dags/sales_data.csv')

    
    data = data.drop_duplicates()
    data['profit_margin'] = (data['revenue'] - data['cost']) / data['revenue']

    print("Données transformées :")
    print(data)

with DAG(
    dag_id='etl_pipeline_example',
    default_args=default_args,
    description='Un pipeline ETL simple',
    schedule_interval='@daily',
    start_date=datetime(2024, 6, 1),
    catchup=False
) as dag:

    task_transform = PythonOperator(
        task_id='extract_and_transform',
        python_callable=extract_and_transform
    )

    task_transform