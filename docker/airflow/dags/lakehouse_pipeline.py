"""DAG dieu phoi pipeline LakeHouse. TODO Giai doan 7: them task that."""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="lakehouse_pipeline",
    description="Pipeline LakeHouse: ingest -> Spark/Iceberg -> dbt -> Superset",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["lakehouse"],
) as dag:

    hello = BashOperator(
        task_id="hello_lakehouse",
        bash_command='echo "Pipeline LakeHouse san sang"',
    )

    hello
