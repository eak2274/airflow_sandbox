from datetime import datetime,timedelta
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "eak74",
    "retries": 5,
    "retry_delay": timedelta(minutes=1)
}


with DAG(
    dag_id="dag_with_minio_s3_v2",
    default_args=default_args,
    start_date=datetime.now() - timedelta(days=2),
    schedule_interval="@daily"
) as dag:
    task1 = S3KeySensor(
        task_id="sensor_minio_s3",
        bucket_name="airflow",
        bucket_key="data_for_minio.csv",
        aws_conn_id="minio_conn",
        mode="poke",
        poke_interval=5,
        timeout=30
    )

    task1