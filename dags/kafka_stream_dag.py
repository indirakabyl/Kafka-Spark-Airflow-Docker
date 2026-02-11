from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka_streaming_service import initiate_stream

DAG_DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="name_stream_dag",
    default_args=DAG_DEFAULT_ARGS,
    schedule="0 1 * * *",
    catchup=False,
    max_active_runs=1,
    description="Stream random names to Kafka topic",
) as dag:
    PythonOperator(
        task_id="stream_to_kafka_task",
        python_callable=initiate_stream,
    )
