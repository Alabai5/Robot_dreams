from datetime import datetime
from datetime import timedelta
from datetime import date
import json
import requests
import json
import os
import yaml
from requests.exceptions import HTTPError

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from hdfs import InsecureClient




def main(params=date.today()):
    client = InsecureClient(f'http://127.0.0.1:50070', user='user')
    #create directiry in HDFS
    client.makedirs('/bronze/RobotDreamsAPI/{0}'.format(params))

    url = "https://robot-dreams-de-api.herokuapp.com/auth"
    headers = {"content-type": "application/json"}
    data = {"username": "rd_dreams", "password": "djT6LasE"}
    r = requests.post(url, headers=headers, data=json.dumps(data))
    token = r.json()['access_token']

    client.delete(os.path.join('/', '/bronze/RobotDreamsAPI/{0}'.format(params), f'Product[{params}].json'), recursive=False)

    try:
        #os.makedirs(os.path.join("./data/", date), exist_ok=True)
        url = "https://robot-dreams-de-api.herokuapp.com/out_of_stock"
        headers = {"content-type": "application/json", "Authorization": "JWT " + token}
        d = {"date": params}
        response = requests.get(url, headers=headers, data=json.dumps(d, default=str))
        response.raise_for_status()
        data = response.json()
        client.write(os.path.join('/', '/bronze/RobotDreamsAPI/{0}'.format(params), f'Product-{params}.json'), data=json.dumps(data), encoding='utf-8')

    except HTTPError:
        print('Date does not exists in the API')
    print(date)

dag_rd = DAG(
    dag_id='copy_data_from_API_to_hdfs',
    description='connection to robot dreams API',
    start_date=datetime(2021, 7, 18, 1, 0),
    end_date=datetime(2022, 7, 23, 10, 0),
    schedule_interval='@daily',
)

run_connection_to_api = PythonOperator(
    task_id='get_data_from_API',
    python_callable=main,
    dag=dag_rd
)

run_connection_to_api
