from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    "owner": "eak74",
    "retries": 5,
    "retry_delay": timedelta(minutes=1)
}


@dag(dag_id="dag_based_on_task_api_v2",
     description="this is our first dag",
     default_args=default_args,
     start_date=datetime(2024, 10, 19, 15),
     schedule_interval="@daily")
def dag_based_on_taskflow_api():
    pass

    @task(multiple_outputs=True)
    def push_name_and_age():
        return {
            "name": "Ann",
            "age": 20
        }

    @task()
    def pull_name_and_age(name: str, age: int):
        print(f"I'm {name}, amd I'm {age} years old")

    name_and_age = push_name_and_age()
    
    pull_name_and_age(name=name_and_age['name'], age=name_and_age['age'])


dag_taskflow_api = dag_based_on_taskflow_api()
