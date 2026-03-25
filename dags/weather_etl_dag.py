from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

INPUT_PATH = "/opt/airflow/dags/data/weather_data.csv"

RAW_PATH = "/tmp/raw_weather.csv"
CLEAN_PATH = "/tmp/clean_weather.csv"
FINAL_PATH = "/tmp/final_weather.csv"


# -------------------
# TASK 1 : EXTRACTION
# -------------------
def extract():
    df = pd.read_csv(INPUT_PATH)
    df.to_csv(RAW_PATH, index=False)
    print("Extraction OK")


# -------------------
# TASK 2 : TRANSFORMATION
# -------------------
def transform():
    df = pd.read_csv(RAW_PATH)

    # 1. Remplacer valeurs nulles
    df["temperature"] = df["temperature"].fillna(df["temperature"].mean())
    df["humidity"] = df["humidity"].fillna(df["humidity"].mean())
    df["wind_speed"] = df["wind_speed"].fillna(df["wind_speed"].mean())

    df["weather_condition"] = df["weather_condition"].fillna("Unknown")

    # 2. Supprimer lignes encore vides (si existantes)
    df = df.dropna()

    # 3. colonne calculée
    df["feels_like_temp"] = df["temperature"] - ((100 - df["humidity"]) / 5)

    df.to_csv(CLEAN_PATH, index=False)
    print("Transformation OK")


# -------------------
# TASK 3 : LOAD
# -------------------
def load():
    df = pd.read_csv(CLEAN_PATH)
    df.to_csv(FINAL_PATH, index=False)
    print("Saved in /tmp")


# -------------------
# DAG
# -------------------
default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="weather_etl_dag_v2",
    schedule_interval=None,
    catchup=False,
    default_args=default_args
) as dag:

    t1 = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    t2 = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    t3 = PythonOperator(
        task_id="load",
        python_callable=load
    )

    t1 >> t2 >> t3