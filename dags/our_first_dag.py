from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def put_name():
    return {"first_name": "Dasha", "family_name": "Ivanova"}
    # return "Dasha"


def get_name(**kwargs):
    first_name = kwargs['ti'].xcom_pull(task_ids='put_name')['first_name']
    family_name = kwargs['ti'].xcom_pull(task_ids='put_name')['family_name']
    print(f"Received first name: {first_name}")
    print(f"Received family name: {family_name}")


def push_name(**kwargs):
    kwargs['ti'].xcom_push(key='mother', value='Alice')
    kwargs['ti'].xcom_push(key='father', value='John')


def pull_name(**kwargs):
    mother_name = kwargs['ti'].xcom_pull(task_ids='push_name', key='mother')
    father_name = kwargs['ti'].xcom_pull(task_ids='push_name', key='father')
    print(father_name, mother_name)


default_args = {
    "owner": "eak74",
    "retries": 5,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    dag_id="our_first_dag_v7",
    description="this is our first dag",
    default_args=default_args,
    start_date=datetime(2024, 10, 19, 15),
    schedule_interval="@daily"
)

task1 = BashOperator(
    dag=dag,
    task_id='first_task',
    bash_command="echo this is the first task"
)

task2 = PythonOperator(
    dag=dag,
    task_id='put_name',
    python_callable=put_name
)

task3 = PythonOperator(
    dag=dag,
    task_id='get_name',
    python_callable=get_name
)

task4 = PythonOperator(
    dag=dag,
    task_id='push_name',
    python_callable=push_name
)

task5 = PythonOperator(
    dag=dag,
    task_id='pull_name',
    python_callable=pull_name
)


task1 >> task2 >> task3 >> task4 >> task5
