from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import csv
import logging
from tempfile import NamedTemporaryFile

from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "eak74",
    "retries": 5,
    "retry_delay": timedelta(minutes=1)
}


def pg_to_s3():
    # step 1 extract data from pg db and save it to a text file
    hook = PostgresHook(
        postgres_conn_id="postgres_localhost"
    )
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM orders WHERE date<TO_DATE('2022-06-12', 'YYYY-MM-DD')")
            with NamedTemporaryFile(mode="w") as f:
            # with open("dags/get_orders.csv", "w") as f:
                csv_writer = csv.writer(f)
                csv_writer.writerow(i[0] for i in cursor.description)
                csv_writer.writerows(cursor)
                f.flush()
                logging.info("Data exported from Pg db and saved to a csv file")
                # step 2 write the text file to s3 bucket
                s3_hook = S3Hook(aws_conn_id="minio_conn")
                s3_hook.load_file(
                    filename=f.name,
                    key="orders/get_orders.csv",
                    bucket_name="airflow",
                    replace=True,
                )
                logging.info("Orders file saved to S3 store")
    pass


with DAG(
    dag_id="dag_with_postgres_hook_v5",
    default_args=default_args,
    start_date=datetime.now() - timedelta(days=2),
    schedule_interval="@daily"
) as dag:
    task1 = PythonOperator(
        task_id="get_orders_from_pg",
        python_callable=pg_to_s3
    )
    # task1 = S3KeySensor(
    #     task_id="sensor_minio_s3",
    #     bucket_name="airflow",
    #     bucket_key="data_for_minio.csv",
    #     aws_conn_id="minio_conn",
    #     mode="poke",
    #     poke_interval=5,
    #     timeout=30
    # )
    task1