from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

MYSQL_CONN = "mysql+pymysql://root:root@mysql:3306/test_db"


def extract_csv(**context):
    df = pd.read_csv('/opt/airflow/dags/data/customer_data.csv')
    context['ti'].xcom_push(key='csv_data', value=df.to_json())


def extract_mysql(**context):
    engine = create_engine(MYSQL_CONN)
    df = pd.read_sql("SELECT * FROM orders", engine)
    context['ti'].xcom_push(key='mysql_data', value=df.to_json())


def transform(**context):
    ti = context['ti']

    df_csv = pd.read_json(ti.xcom_pull(key='csv_data'))
    df_mysql = pd.read_json(ti.xcom_pull(key='mysql_data'))

    df = pd.merge(df_csv, df_mysql, on='customer_id')

    df['total_with_tax'] = df['amount'] * 1.2

    ti.xcom_push(key='final_data', value=df.to_json())


def load(**context):
    ti = context['ti']
    df = pd.read_json(ti.xcom_pull(key='final_data'))

    engine = create_engine(MYSQL_CONN)
    df.to_sql('customer_orders_summary', engine, if_exists='replace', index=False)


default_args = {
    'start_date': datetime(2024, 1, 1)
}

with DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='extract_csv_task',
        python_callable=extract_csv
    )

    t2 = PythonOperator(
        task_id='extract_mysql_task',
        python_callable=extract_mysql
    )

    t3 = PythonOperator(
        task_id='transform_task',
        python_callable=transform
    )

    t4 = PythonOperator(
        task_id='load_task',
        python_callable=load
    )

    [t1, t2] >> t3 >> t4