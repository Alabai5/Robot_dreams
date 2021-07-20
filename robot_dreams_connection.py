from datetime import datetime
from datetime import timedelta
import json
import requests
import json
import os
import yaml
from requests.exceptions import HTTPError

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator



dag_rd = DAG(
    dag_id='robot_dreams_http',
    description='connection to robot dreams API',
    start_date=datetime(2021, 7, 18, 15, 0),
    end_date=datetime(2021, 7, 23, 10, 0),
    schedule_interval='@daily',
    params={'date':['2017-01-01', '2017-02-03', '2021-03-09', '2021-03-10']}
)

def app():
    url = "https://robot-dreams-de-api.herokuapp.com/auth"
    headers = {"content-type": "application/json"}
    data = {"username": "rd_dreams", "password": "djT6LasE"}
    r = requests.post(url, headers=headers, data=json.dumps(data))
    token = r.json()['access_token']

    for date in dag_rd.params['date']:
        try:
            os.makedirs(os.path.join("./data/", date), exist_ok=True)
            url = "https://robot-dreams-de-api.herokuapp.com/out_of_stock"
            headers = {"content-type": "application/json", "Authorization": "JWT " + token}
            d = {"date": date}
            response = requests.get(url, headers=headers, data=json.dumps(d))
            response.raise_for_status()
            data = response.json()
            with open(os.path.join("./data/", date, f'Product[{date}].json'), 'w') as f:
                json.dump(data, f)
        except HTTPError:
            print('Date does not exists in the API')
        print(date)

run_connection_to_api = PythonOperator(
    task_id='get_data_from_API',
    python_callable=app,
    ##provide_context=True,
    ##op_kwargs={'startdate': dag_rd.start_date},
    dag=dag_rd
)

run_connection_to_api