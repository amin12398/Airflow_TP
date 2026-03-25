from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG(
    dag_id="sensor_example",
    start_date=datetime(2024, 6, 1),
    schedule_interval=None
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/tmp/data_ready.txt",
        poke_interval=10,
        timeout=300
    )

    file_ready_task = DummyOperator(task_id="file_ready")

    wait_for_file >> file_ready_task