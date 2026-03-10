from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

with DAG(
    dag_id="dbt_transform",
    start_date=datetime(2024,1,1),
    schedule="@daily",
    catchup=False
) as dag:

    dbt_run = DockerOperator(
        task_id="dbt_run",
        image="ghcr.io/dbt-labs/dbt-postgres:1.7.0",
        command="dbt run",
        mounts=["/opt/project/dbt:/usr/app"],
        working_dir="/usr/app",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )
