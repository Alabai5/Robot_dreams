import json
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from datetime import timedelta

import psycopg2
from hdfs import InsecureClient

pg_creds = {
    'host': '172.20.10.7',
    'port': '5432',
    'database':'dshop',
    'user':'pguser',
    'password':'secret'
}
def main(table_name):
    client = InsecureClient(f'http://127.0.0.1:50070', user='user')
    #create directiry in HDFS
    client.makedirs('/bronze/dshop/{0}'.format(table_name))

    #copy table's data from PostgreSQL to HDFS
    client.delete(os.path.join ('/','/bronze/dshop/{0}'.format(table_name),'{0}.csv'.format(table_name)), recursive=False)
    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        with client.write(os.path.join ('/','/bronze/dshop/{0}'.format(table_name),'{0}.csv'.format(table_name)),) as csv_file:
            cursor.copy_expert('COPY (SELECT * FROM {0}) TO STDOUT WITH HEADER CSV'.format(table_name), csv_file)

dag_rd = DAG(
    dag_id='copy_data_from_postgreSQL_to_hdfs',
    description='connection to postgres DB and load data to hdfs',
    start_date=datetime(2021, 7, 18, 1, 0),
    end_date=datetime(2022, 7, 23, 5, 0),
    schedule_interval='@daily',
    params={'list_tables':['aisles', 'clients', 'departments', 'orders', 'products']}
)

for table_name in dag_rd.params['list_tables']:
    copy_data = PythonOperator(
        task_id=f"CopyToHDFS_{table_name}",
        dag=dag_rd,
        python_callable= main,
        op_kwargs={
            "table_name": table_name
        }


    )

copy_data
