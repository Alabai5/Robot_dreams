import os
import psycopg2
import logging

from hdfs import InsecureClient
from airflow.hooks.base_hook import BaseHook
from airflow import DAG
from datetime import date

def load_tables_from_db_to_bronze(table, **context):

    start = context['execution_date']
    ex_date = start.strftime('%Y-%m-%d')
    hdfs_conn = BaseHook.get_connection('DataLake_hdfs')
    pg_conn = BaseHook.get_connection('oltp_postgres')
    pg_creds = {
        'host': pg_conn.host,
        'port': pg_conn.port,
        'user': pg_conn.login,
        'password': pg_conn.password,
        'database': 'dshop_bu'
    }

    logging.info(f"Write table {table} from {pg_conn.host} to Bronze")
    client = InsecureClient(f'http://127.0.0.1:50070', user='user')


    client.delete(os.path.join('/', 'DataLake/bronze/dshop_bu/{0}'.format(table), ex_date, '{0}.csv'.format(table)), recursive=False)
    logging.info(f"Delete exist directory from hadoop")

    logging.info(f"Writing data to {table} split by partitions {ex_date}")
    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        with client.write(os.path.join('/', 'DataLake/bronze','dshop_bu',table, ex_date,'{0}.csv'.format(table)), ) as csv_file:
            cursor.copy_expert(f"COPY {table} TO STDOUT WITH HEADER CSV", csv_file)
    logging.info(f"Successfully loaded")

