from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    dag_id="aviation_operations_pipeline",
    description="Orchestrates the Aviation Operations Data Platform",
    start_date=datetime(2026, 7, 20),
    schedule=None,
    catchup=False,
    tags=["aviation", "opensky", "aws"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_opensky_ingestion = BashOperator(
        task_id="run_opensky_ingestion",
        bash_command="""
        cd /opt/airflow/aviation &&
        python main.py
        """,
    )

    end = EmptyOperator(task_id="end")

    start >> run_opensky_ingestion >> end