from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import mysql.connector
import json

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}



# -----------------------
# EXTRACT CSV
# -----------------------
def extract_csv(**context):
    df_csv = pd.read_csv('/opt/airflow/dags/customers_data.csv')

    context['ti'].xcom_push(
        key='csv_data',
        value=df_csv.to_json(orient='records')
    )

def extract_mysql(**context):
    conn = mysql.connector.connect(
        host="mysql",
        user="root",
        password="root",
        database="airflow"
    )

    df_mysql = pd.read_sql("SELECT * FROM orders", conn)
    conn.close()

    context['ti'].xcom_push(
        key='mysql_data',
        value=df_mysql.to_json(orient='records')
    )


def transform(**context):
    csv_data = json.loads(context['ti'].xcom_pull(key='csv_data'))
    mysql_data = json.loads(context['ti'].xcom_pull(key='mysql_data'))

    df_csv = pd.DataFrame(csv_data)
    df_mysql = pd.DataFrame(mysql_data)

    merged = pd.merge(df_csv, df_mysql, on="customer_id", how="inner")

    merged['total_value'] = merged['amount'] * merged['age']

    context['ti'].xcom_push(
        key='final_data',
        value=merged.to_json(orient='records')
    )


def load_to_mysql(**context):
    data = json.loads(context['ti'].xcom_pull(key='final_data'))
    df = pd.DataFrame(data)

    conn = mysql.connector.connect(
        host="mysql",
        user="root",
        password="root",
        database="airflow"
    )

    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS merged_results (
            customer_id INT,
            name VARCHAR(255),
            age INT,
            city VARCHAR(255),
            order_id INT,
            amount INT,
            total_value FLOAT
        )
    """)

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO merged_results
            (customer_id, name, age, city, order_id, amount, total_value)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (
            row['customer_id'],
            row['name'],
            row['age'],
            row['city'],
            row['order_id'],
            row['amount'],
            row['total_value']
        ))

    conn.commit()
    conn.close()


with DAG(
    dag_id="csv_mysql_merge_etl",
    default_args=default_args,
    description="Fusion CSV + MySQL + transformation + load",
    schedule_interval=None,
    start_date=datetime(2024, 6, 1),
    catchup=False
) as dag:

    task0 = PythonOperator(
        task_id="create_orders_table",
        python_callable=create_orders_table
    )

    task1 = PythonOperator(
        task_id="extract_csv",
        python_callable=extract_csv
    )

    task2 = PythonOperator(
        task_id="extract_mysql",
        python_callable=extract_mysql
    )

    task3 = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    task4 = PythonOperator(
        task_id="load_to_mysql",
        python_callable=load_to_mysql
    )

    task0 >> task1 >> task2 >> task3 >> task4