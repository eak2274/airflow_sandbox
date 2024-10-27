from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import matplotlib


def print_something():
    print("This is something")
    print()
    print("matplotlib version is", matplotlib.__version__)


default_args = {
    "owner": "eak74",
    "retries": 5,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    dag_id="dependency_dag_v3",
    description="this is our first dag",
    default_args=default_args,
    start_date=datetime.now() - timedelta(days=2),
    schedule_interval="@daily"
)

task1 = PythonOperator(
    dag=dag,
    task_id='put_name',
    python_callable=print_something
)

task1