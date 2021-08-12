import os
import json
import requests
import logging
from requests.exceptions import HTTPError

from hdfs import InsecureClient
from airflow.hooks.base_hook import BaseHook
from datetime import date
from airflow import DAG
from airflow.models import DagBag
from airflow.models.dagrun import DagRun
def load_data_from_api_to_bronze(list_params):


    params = str(date.today())
    client = InsecureClient(f'http://127.0.0.1:50070', user='user')
    client.makedirs('/DataLake/bronze/RobotDreamsAPI/{0}'.format(params))

    api_conn = BaseHook.get_connection('http_robot_dreams_api_connection')
    auth = list_params['endpoint_auth'][0]
    url = api_conn.host+auth
    headers = {"content-type": list_params['headers']['content-type'][0]}
    data = {"username": list_params['data']['username'][0], "password": list_params['data']['password'][0]}
    r = requests.post(url, headers=headers, data=json.dumps(data))
    token = r.json()['access_token']

    logging.info(f"Writing file Product-{params}.json from {api_conn.host} to Bronze")
    client.delete(os.path.join('/', 'DataLake/bronze/RobotDreamsAPI/{0}'.format(params), f'Product[{params}].json'),
                  recursive=False)
    endpoint_api = list_params['endpoint_api'][0]
    url = api_conn.host + endpoint_api
    headers = {"content-type": list_params['headers']['content-type'][0], "Authorization": list_params['headers']['Authorization'][0] + " " + token}
    d = {"date": params}
    response = requests.get(url, headers=headers, data=json.dumps(d, default=str))
    response.raise_for_status()
    data = response.json()
    client.write(os.path.join('/', 'DataLake/bronze/RobotDreamsAPI/{0}'.format(params), f'Product-{params}.json'),
                 data=json.dumps(data), encoding='utf-8')
    logging.error("Data doesn't find {0} into {1}".format(HTTPError, api_conn.host))

