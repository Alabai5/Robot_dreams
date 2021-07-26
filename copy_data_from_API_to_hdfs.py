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


params = date.today()

def main(params):
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


if __name__ == '__main__':
    main(params)
