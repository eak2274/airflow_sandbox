from datetime import datetime,timedelta
from airflow import DAG

from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "eak74",
    "retries": 5,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="dag_with_postgres_operator_v8",
    default_args=default_args,
    start_date=datetime.now() - timedelta(days=2),
    schedule_interval="@daily"
) as dag:
    task1 = PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id="postgres_localhost",
        sql="""
        create table if not exists dag_runs (
            ts timestamp,
            dag_id character varying,
            primary key (ts, dag_id)
        )
        """
    )

    task2 = PostgresOperator(
        task_id="log_execution",
        postgres_conn_id="postgres_localhost",
        sql="""
                INSERT INTO dag_runs (ts, dag_id) VALUES (%(ts)s, %(dag_id)s);
            """,
        parameters={
            "ts": "{{ ts }}",
            "dag_id": "{{ dag.dag_id }}"
        }
    )

    task1 >> task2