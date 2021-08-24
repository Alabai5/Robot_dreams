import os
import yaml


from datetime import datetime
from datetime import date
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from functions.load_tables_dshop_db_from_bronze_to_silver_spark import load_tables_dshop_db_from_bronze_to_silver_spark
from functions.load_tables_dshop_from_silver_to_dwh_spark import load_tables_dshop_from_silver_to_dwh
from airflow import DAG

default_args = {
    "owner": "airflow",
    "email_on_failure": False
}
def return_tables():
    with open(os.path.join('/home/user/airflow/dags/', 'config.yaml'), 'r') as yaml_file:
        config = yaml.safe_load((yaml_file))
        return config.get('daily_etl').get('sources').get('postgresql_dshop')

def load_to_silver_group(value):
    return PythonOperator(
        task_id="load_table_" + value + "_from_bronze_to_silver",
        python_callable=load_tables_dshop_db_from_bronze_to_silver_spark,
        op_kwargs={"table": value},
        provide_context=True

    )

def load_to_dwh_group(value):
    return PythonOperator(
        task_id="load_table_" + value + "_to_olap",
        python_callable=load_tables_dshop_from_silver_to_dwh,
        op_kwargs={"table": value}
    )


dag = DAG (
    dag_id="load_tables_dshopBU_to_silver_and_dwh",
    description="Load data from bronze HDFS DShop to Data Lake silver",
    schedule_interval="@daily",
    start_date=datetime(2021, 8, 10, 2, 0),
    default_args = default_args
)

dummy1 = DummyOperator(
    task_id='start_load_tables_from_bronze_to_silver',
    dag=dag
)

dummy2 = DummyOperator(
    task_id='finish_load_tables_from_bronze_to_silver',
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

for table in return_tables():
    dummy1 >> load_to_silver_group(table) >> dummy2 >> dummy3 >> load_to_dwh_group(table) >> dummy4