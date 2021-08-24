import os
import yaml


from datetime import datetime
from datetime import date
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from functions.load_tables_from_db_to_bronze import load_tables_from_db_to_bronze
from functions.load_tables_dshop_db_from_bronze_to_silver_spark import load_tables_dshop_db_from_bronze_to_silver_spark
from airflow import DAG


default_args = {
    "owner": "airflow",
    "email_on_failure": False
}

def return_tables():
    with open(os.path.join('/home/user/airflow/dags/', 'config.yaml'), 'r') as yaml_file:
        config = yaml.safe_load((yaml_file))
        return config.get('daily_etl').get('sources').get('postgresql_dshop')

def load_to_bronze_group(value):
    return PythonOperator(
        task_id="load_table_"+value+"_from_dshop_to_bronze",
        python_callable=load_tables_from_db_to_bronze,
        op_kwargs={"table":value},
        provide_context=True,
        dag=dag
    )


dag = DAG (
    dag_id="load_tables_from_db_dshopBU_to_bronze",
    description="Load data from postgresSQL DShop data base to Data Lake bronze",
    schedule_interval="@daily",
    start_date=datetime(2021, 8, 10),
    default_args = default_args
)

dummy1 = DummyOperator(
    task_id='start_load_tables_from_db_to_bronze',
    dag=dag
)

dummy2 = DummyOperator(
    task_id='finish_load_tables_from_db_to_bronze',
    dag=dag
)



for table in return_tables():
    dummy1 >> load_to_bronze_group(table) >> dummy2