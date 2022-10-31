import requests
import json
import time

from datetime import datetime

url = '${url}'
table_name = '${table_name}'

etl = ${etl}
etl['stage'] = 'extract'
etl['start_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
etl['meta']['table_name'] = table_name

for k in range(3):
    response = requests.request("GET", url)
    if response.status_code == 200:
        data = json.loads(response.content)
        etl['logs'][f"Try {k}"] = "Data was extracted successfully"
        break
    else:
        etl['logs'][f"Try {k}"] = f"Status code of response is {response.status_code}"
        time.sleep(5)

etl['data'] = data
etl['end_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

print(json.dumps(etl))