import requests
import json
import os
from config import Config
import yaml
from requests.exceptions import HTTPError

def app(config):

    url = config['url'] + config['endpoint_auth']
    headers = {"content-type": config['content-type']}
    data = {"username": config['username'], "password": config['password']}
    r = requests.post(url, headers=headers, data=json.dumps(data))
    token = r.json()['access_token']

    for date in config['payload_date']:
        try:
            os.makedirs(os.path.join(config['directory'], date), exist_ok=True)
            url = config['url'] + config['endpoint_stock']
            headers = {"content-type": config['content-type'], "Authorization": config['Authorization'] + token}
            d = {"date": date}
            response = requests.get(url, headers=headers, data=json.dumps(d))
            response.raise_for_status()
            data = response.json()
            with open(os.path.join(config['directory'], date, f'Product[{date}].json'), 'w') as f:
                json.dump(data, f)
        except HTTPError:
            print('Date does not exists in the API')

if __name__ == '__main__':
    config = Config(os.path.join('.', 'config.yaml'))
    app(
            config=config.get_config('robot_dream_date_app')
    )

