import os
import yaml


from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from functions.load_data_from_api_to_bronze import load_data_from_api_to_bronze
from functions.load_data_rd_api_from_bronze_to_silver_spark import load_data_rd_api_from_bronze_to_silver_spark
from functions.load_data_from_outOfStock_silver_to_dwh import load_data_outOfStock_to_dwh
from airflow import DAG
from airflow.configuration import conf

default_args = {
    "owner": "airflow",
    "email_on_failure": False
}

def load_data_from_bronze_api_to_silver_group():
    return PythonOperator(
        task_id="load_data_from_api_to_silver",
        python_callable=load_data_rd_api_from_bronze_to_silver_spark,
        provide_context=True
    )

def load_to_dwh_group():
    return PythonOperator(
        task_id="load_data_outOfStock_to_olap",
        python_callable=load_data_outOfStock_to_dwh
    )

dag = DAG (
    dag_id="load_data_outOfstock_bronze_to_silver",
    description="Load data from API to Data Lake bronze and silver",
    schedule_interval="@daily",
    start_date=datetime(2021, 8, 10, 2, 0),
    default_args=default_args

)

dummy1 = DummyOperator(
    task_id='start_load_data_from_bronze_to_silver_and_olap',
    dag=dag
)

dummy2= DummyOperator(
    task_id='finish_load_data_from_bronze_to_silver',
    dag=dag
)

dummy3 = DummyOperator(
    task_id='start_load_tables_from_silver_to_dwh',
    dag=dag
)

dummy4 = DummyOperator(
    task_id='finish_load_tables_from_silver_to_dwh',
    dag=dag
)


dummy1 >> load_data_from_bronze_api_to_silver_group() >> dummy2 >> dummy3 >> load_to_dwh_group() >> dummy4