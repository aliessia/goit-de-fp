from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='batch_datalake_pipeline',
    default_args=default_args,
    schedule_interval=None,
    description='ETL pipeline for batch datalake',
) as dag:

    landing_to_bronze = SparkSubmitOperator(
        task_id='landing_to_bronze',
        application='/Users/alesyasoloviova/PycharmProjects/batch_datalake_project/dags/alesya/landing_to_bronze.py',
        conn_id='spark_default',
        conf={"spark.master": "local[*]"}
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application='/Users/alesyasoloviova/PycharmProjects/batch_datalake_project/dags/alesya/bronze_to_silver.py',
        conn_id='spark_default',
        conf={"spark.master": "local[*]"}
    )

    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application='/Users/alesyasoloviova/PycharmProjects/batch_datalake_project/dags/alesya/silver_to_gold.py',
        conn_id='spark_default',
        conf={"spark.master": "local[*]"}
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
