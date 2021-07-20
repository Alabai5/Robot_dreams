from airflow import models
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

import psycopg2
from datetime import datetime
from datetime import timedelta
import os

POSTGRES_CONN_ID = 'postgres_default'

def copy_to_file(table_name,file_name):
    pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)
    os.makedirs(os.path.join("./data/", table_name), exist_ok=True)
    pg_hook.bulk_dump(table_name,  os.path.join("./data/", table_name, file_name))

dag_rd = DAG(
    dag_id='Copy_From_DB_dshop_To_File',
    description='connection to postgres DB and load data to file',
    start_date=datetime(2021, 7, 18, 15, 0),
    end_date=datetime(2021, 7, 23, 10, 0),
    schedule_interval='@daily',
    params={'list_tables':['aisles', 'clients', 'departments', 'orders', 'products']}
)

for table in dag_rd.params['list_tables']:
    copy_data = PythonOperator(
        task_id=f"CopyToFile_{table}",
        dag=dag_rd,
        python_callable=copy_to_file,
        op_kwargs={
            "table_name":table,
            "file_name":"{}.csv".format(table)
        }

)

copy_data