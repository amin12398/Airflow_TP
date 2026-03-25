from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import mysql.connector
import os

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


def extract_csv(**context):
    file_path = "/opt/airflow/dags/customers_data.csv"

    context["ti"].xcom_push(
        key="csv_path",
        value=file_path
    )


def extract_mysql(**context):
    conn = mysql.connector.connect(
        host="mysql",
        user="root",
        password="root",
        database="airflow"
    )

    df = pd.read_sql("SELECT * FROM orders", conn)
    conn.close()

    path = "/opt/airflow/data/mysql_orders.csv"
    df.to_csv(path, index=False)

    context["ti"].xcom_push(
        key="mysql_path",
        value=path
    )

def transform(**context):
    csv_path = context["ti"].xcom_pull(key="csv_path")
    mysql_path = context["ti"].xcom_pull(key="mysql_path")

    df_csv = pd.read_csv(csv_path)
    df_mysql = pd.read_csv(mysql_path)

    merged = pd.merge(df_csv, df_mysql, on="customer_id", how="inner")

    merged["total_amount"] = merged["amount"] + 25

    output_path = "/opt/airflow/data/final_result.csv"
    merged.to_csv(output_path, index=False)

    context["ti"].xcom_push(
        key="final_path",
        value=output_path
    )


def load(**context):
    path = context["ti"].xcom_pull(key="final_path")

    df = pd.read_csv(path)

    conn = mysql.connector.connect(
        host="mysql",
        user="root",
        password="root",
        database="airflow"
    )

    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS final_results (
            customer_id INT,
            name VARCHAR(255),
            city VARCHAR(255),
            order_id INT,
            amount INT,
            total_amount INT
        )
    """)

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO final_results
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (
            row["customer_id"],
            row["name"],
            row["city"],
            row["order_id"],
            row["amount"],
            row["total_amount"]
        ))

    conn.commit()
    conn.close()


with DAG(
    dag_id="xcom_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 6, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id="extract_csv",
        python_callable=extract_csv
    )

    t2 = PythonOperator(
        task_id="extract_mysql",
        python_callable=extract_mysql
    )

    t3 = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    t4 = PythonOperator(
        task_id="load",
        python_callable=load
    )

    t1 >> [t2] >> t3 >> t4