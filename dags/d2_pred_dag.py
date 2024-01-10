import json
import requests
import time

from copy import deepcopy

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

default_args = {
    "owner": "Vladlen",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}


def read_file(variable):
    path = Variable.get(variable)
    with open(path, 'r') as f:
        file = json.load(f)
    return file


def execute_query(query):
    pg_hook = PostgresHook(
        postgres_conn_id='d2_dwh',
        schema='d2_dwh'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(query)
    returned_value = cursor.fetchall()
    pg_conn.commit()
    return returned_value


def initialize_job(ti):
    try:
        etl = read_file('etl_path')
        print("ETL-file has been loaded successfully")
        etl = json.dumps(etl)
        etl = str(etl).replace("'", "''")
        print("Creating a new job...")
        query = f"select service.create_job('{etl}');"
        job_uid = execute_query(query)[0][0]
        print("Job has been created successfully")
        ti.xcom_push(key='job_uid', value=job_uid)
    except BaseException as e:
        print(f"ERROR: {e}")


def extract_data(ti):
    priority = read_file('priority_path')
    job_uid = ti.xcom_pull(task_ids='initialize_job', key='job_uid')
    query = f"select etl from service.jobs where uid = '{job_uid}'"
    etl = execute_query(query)[0][0]
    try:
        package_size = int(Variable.get('package_size'))
        print(f"Package size: {package_size}")
    except Exception as e:
        print(f"Error getting package size: {e.message}")
    etl['meta']['data_package_size'] = package_size
    etl['meta']['job_uid'] = job_uid
    for item in priority:
        etl_item = deepcopy(etl)
        # etl['stage'] = 'extract'
        table_name = item['table_name']
        url = item['url']
        # etl['start_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # print(etl['meta']['table_name'])
        etl_item['meta']['table_name'] = table_name
        if '<id>' not in url:
            if table_name == 'teams':
                page = 0
                data = []
                while True:  # TODO: доделать, не совсем нравится
                    url = item['url']
                    etl_item_task = deepcopy(etl_item)
                    url += f'?page={page}'
                    for k in range(3):
                        response = requests.request("GET", url)
                        if response.status_code == 200:
                            etl_item_task['logs'][f"Try: {k + 1}"] = "Data was extracted successfully"
                            break
                        else:
                            etl_item_task['logs'][
                                f"Try: {k + 1}"] = f"Status code of response is {response.status_code}"
                            time.sleep(5)
                    page_data = json.loads(response.content)
                    if isinstance(page_data, dict):
                        time.sleep(3)
                        continue
                    if len(page_data) != 0:
                        etl_item_task['data'] = page_data
                        etl_item_task = json.dumps(etl_item_task)
                        etl_item_task = str(etl_item_task).replace("'", "''")
                        query = f"select service.create_package('{etl_item_task}');"
                        execute_query(query)
                        page += 1
                        time.sleep(3)
                    else:
                        break
                continue

            for k in range(3):
                response = requests.request("GET", url)
                if response.status_code == 200:
                    data = json.loads(response.content)
                    etl_item['logs'][f"Try: {k + 1}"] = "Data was extracted successfully"
                    break
                else:
                    etl_item['logs'][f"Try: {k + 1}"] = f"Status code of response is {response.status_code}"
                    time.sleep(5)
        else:
            # TODO: Create script that processes url with <ids>
            matches_data = requests.request("GET", 'https://api.opendota.com/api/proMatches')
            for match in matches_data:
                match_id = match['id']
                for k in range(3):
                    response = requests.request("GET", url.replace('<id>', id))
                    if response.status_code == 200:
                        match_data = json.loads(response.content)
                        etl_item['logs'][f"Try: {k + 1}"] = "Data was extracted successfully"
                        break
                    else:
                        etl_item['logs'][f"Try: {k + 1}"] = f"Status code of response is {response.status_code}"
                        time.sleep(5)
                data.append(match_data)
            etl_item['logs']["Try: 1"] = "Data was extracted successfully"

        etl_item['data'] = data
        # etl['end_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        etl_item = json.dumps(etl_item)
        etl_item = str(etl_item).replace("'", "''")
        query = f"select service.create_package('{etl_item}');"
        execute_query(query)


def transform_data(ti):
    mapping = read_file('mapping_path')
    job_uid = ti.xcom_pull(task_ids='initialize_job', key='job_uid')
    query = f"select p.table_name, p.uid from service.packages p " \
            f"left join service.jobs j on p.job_id = j.id " \
            f"where j.uid = '{job_uid}';"
    packages_info = execute_query(query)

    default_values = ['', '-', '—']
    for table_name, package_uid in packages_info:
        query = f"select package from service.packages where uid = '{package_uid}'"
        [[data]] = execute_query(query)
        data_transformed = []
        for table_mapping in mapping:
            if table_mapping['table_name_source'] == table_name:
                for row in data:
                    object = {
                        'table_name_dwh': table_mapping['table_name_dwh'],
                        'unique_field': table_mapping['unique_field']
                    }
                    for field_source, field_dwh in table_mapping['fields'].items():
                        value = row[field_source].replace('\"', "'") if isinstance(row[field_source], str) else row[
                            field_source]
                        if table_mapping['transformations'][field_source] is not None:
                            to_eval = table_mapping['transformations'][field_source].replace('<value>', str(value))
                            value = eval(to_eval)
                        object[field_dwh] = value if value not in default_values else None
                    data_transformed.append(object)
        etl = {
            "stage": "transform",
            "meta": {
                "table_name": table_name,
                "package_uid": package_uid
            },
            "data": data_transformed,
            "logs": {"Try: 0": "Data was successfully transformed"}
        }
        etl = json.dumps(etl)
        etl = str(etl).replace("'", "''")
        query = f"select service.insert_transformed_object('{etl}');"
        execute_query(query)


def insert_data(ti):
    priority = read_file('priority_path')
    job_uid = ti.xcom_pull(task_ids='initialize_job', key='job_uid')
    query = f"select p.id from service.packages p " \
            f"left join service.jobs j on p.job_id = j.id " \
            f"where j.uid = '{job_uid}';"
    package_ids = execute_query(query)
    for table in priority:
        table_name = table['table_name']
        for (package_id,) in package_ids:
            query = f"select object from service.objects where package_id = {package_id} and table_name = '{table_name}';"
            data = execute_query(query)
            etl = {
                "stage": "insert",
                "meta": {
                    "table_name": table_name
                },
                "data": data,
                "logs": {"Try: 0": "Data was successfully sent to insert"}
            }
            etl = json.dumps(etl)
            etl = str(etl).replace("'", "''''")
            query = f"select data.insert_data('{etl}');"
            print(execute_query(query))


with DAG(
        dag_id='D2_ETL',
        default_args=default_args,
        start_date=datetime(2022, 10, 28, 1),
        schedule_interval='@daily'
) as dag:
    initialize_job = PythonOperator(
        task_id='initialize_job',
        python_callable=initialize_job
    )

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )
    insert_data = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data
    )

    initialize_job >> extract_data >> transform_data >> insert_data
