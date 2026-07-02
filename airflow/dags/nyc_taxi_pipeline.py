import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

SPARK_IMAGE = os.environ["SPARK_IMAGE"]
DBT_IMAGE = os.environ["DBT_IMAGE"]
DOCKER_NETWORK = os.environ["DOCKER_NETWORK"]

JOB_ENV = {
    "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
    "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
    "AWS_REGION": os.environ["AWS_REGION"],
    "S3_ENDPOINT": os.environ["S3_ENDPOINT"],
    "ICEBERG_CATALOG_URI": os.environ["ICEBERG_CATALOG_URI"],
    "ICEBERG_WAREHOUSE": os.environ["ICEBERG_WAREHOUSE"],
    "LAKEHOUSE_BUCKET": os.environ["LAKEHOUSE_BUCKET"],
    "TLC_DATASET": os.environ["TLC_DATASET"],
    "TLC_MONTHS": os.environ["TLC_MONTHS"],
    "PYTHONPATH": "/opt/spark_jobs",
}


def docker_task(task_id, image, command, environment=None):
    return DockerOperator(
        task_id=task_id,
        image=image,
        command=command,
        environment=environment or {},
        network_mode=DOCKER_NETWORK,
        auto_remove="success",
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
    )


def spark_task(task_id, entrypoint):
    return docker_task(task_id, SPARK_IMAGE, entrypoint, JOB_ENV)


with DAG(
    dag_id="nyc_taxi_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule="@monthly",
    catchup=False,
    default_args={"retries": 1},
    tags=["lakehouse", "iceberg", "spark"],
) as dag:
    download = spark_task("download_tlc", "python3 /opt/ingestion/download_tlc.py")
    ingest_bronze = spark_task("ingest_bronze", "spark-submit /opt/spark_jobs/ingest_bronze.py")
    bronze_to_silver = spark_task(
        "bronze_to_silver", "spark-submit /opt/spark_jobs/bronze_to_silver.py"
    )
    silver_to_gold = spark_task(
        "silver_to_gold", "spark-submit /opt/spark_jobs/silver_to_gold.py"
    )
    data_quality = spark_task(
        "data_quality", "spark-submit /opt/spark_jobs/data_quality.py"
    )
    dbt_build = docker_task(
        "dbt_build", DBT_IMAGE, "dbt build --profiles-dir . --target prod"
    )

    download >> ingest_bronze >> bronze_to_silver >> silver_to_gold >> data_quality >> dbt_build
