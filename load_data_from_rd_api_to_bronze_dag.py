import os
import yaml


from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from functions.load_data_from_api_to_bronze import load_data_from_api_to_bronze
from functions.load_data_rd_api_from_bronze_to_silver_spark import load_data_rd_api_from_bronze_to_silver_spark
from airflow import DAG
from airflow.configuration import conf

default_args = {
    "owner": "airflow",
    "email_on_failure": False
}


def load_data_to_bronze_api_group():
    with open(os.path.join('/home/user/airflow/dags/', 'config.yaml'), 'r') as yaml_file:
        config = yaml.safe_load((yaml_file))
        api_list = config.get('daily_etl').get('sources').get('api')
    return PythonOperator(
        task_id="load_data_from_api_to_bronze",
        python_callable=load_data_from_api_to_bronze,
        op_kwargs={"list_params": api_list}
    )

def load_data_from_bronze_api_to_silver_group():
    return PythonOperator(
        task_id="load_data_from_api_to_silver",
        python_callable=load_data_rd_api_from_bronze_to_silver_spark
    )

dag = DAG (
    dag_id="load_data_from_api_to_bronze_and_silver",
    description="Load data from API to Data Lake bronze and silver",
    schedule_interval="@daily",
    start_date=datetime(2021, 8, 10),
    default_args=default_args,

)

dummy1 = DummyOperator(
    task_id='start_load_data_from_api_to_bronze',
    dag=dag
)

dummy2 = DummyOperator(
    task_id='finish_load_data_from_api_to_bronze',
    dag=dag
)

dummy3 = DummyOperator(
    task_id='start_load_data_from_to_bronze_to_silver',
    dag=dag
)

dummy4 = DummyOperator(
    task_id='finish_load_data_from_bronze_to_silver',
    dag=dag
)

dummy1 >> load_data_to_bronze_api_group() >> dummy2 >> dummy3 >> load_data_from_bronze_api_to_silver_group() >> dummy4