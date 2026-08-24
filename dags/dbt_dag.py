from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dbt_dag",
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    # Task 1: Run your dbt models
    dbt_run = BashOperator(
        task_id="dbt_run",
        # Point Airflow to where your dbt folder lives inside the Docker container
        bash_command="dbt run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project"
    )

    # Task 2: Run your dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project"
    )

    # Set the order, run the models, then test the data
    dbt_run >> dbt_test