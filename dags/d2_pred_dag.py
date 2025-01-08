import json
import requests
import time

from copy import deepcopy

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

from urllib.parse import quote_plus, quote
from datetime import datetime
from dateutil.relativedelta import relativedelta

default_args = {
    "owner": "Vladlen",
    "retries": 50,
    "retry_delay": timedelta(seconds=30)
}


def read_file(variable):
    path = Variable.get(variable)
    with open(path, 'r') as f:
        file = json.load(f)
    return file


def execute_query(query, fetch=True):
    pg_hook = PostgresHook(
        postgres_conn_id='d2_dwh'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(query)
    if fetch:
        returned_value = cursor.fetchall()
        pg_conn.commit()
    else:
        pg_conn.commit()
        return
    return returned_value


def initialize_job(ti):
    """
    DAG TASK
    Creates a new job in service.jobs table and pushes its uuid to the XCom
    :param ti: XCom argument
    """
    try:
        job_code = 'dota2_etl'
        print("Creating a new job...")
        query = f"select service.create_job('{job_code}');"
        job_uid = execute_query(query)[0][0]
        print("Job has been created successfully!")
        ti.xcom_push(key='job_uid', value=job_uid)
    except BaseException as e:
        print(f"ERROR: {e}")


def query_insert_package(etl_json_item, data):
    """
    Creates a new package in service.packages table with the given JSON
    :param etl_json_item: header of result JSON
    :param data: data that was extracted from the source
    """
    if len(data) >= 0:
        etl_json_item['data'] = data

        etl_json_item = json.dumps(etl_json_item)
        etl_json_item = str(etl_json_item).replace("'", "''")
        query = f"select service.create_package('{etl_json_item}');"
        execute_query(query)
        print("Package has been created successfully!")
    else:
        return "Data is empty!"

def request_data(url):
    """
    Requests data based on the provided URL. If response is not successful, try again after a delay. Raise an exception
    if number of tries exceeds count limit
    :param url: Query string of request
    :return: JSON of the response
    """
    print("Requesting data...")
    count = 0
    while count < 10:
        response = requests.request("GET", url)
        if response.status_code != 200:
            print(f"ERROR! Response code is {response.status_code}, response message: {response.text}")
            count += 1
            time.sleep(5)
            continue
        print("Data has been retrieved successfully!")
        return json.loads(response.content)
    raise ValueError("ERROR requesting data")


def extract_data(ti):
    """
    DAG TASK
    Extracts data from API and inserts packages to service.packages
    :param ti: XCom argument
    """
    priority = read_file('priority_path')
    job_uid = ti.xcom_pull(task_ids='initialize_job', key='job_uid')

    # Size of package that data shpuld be split on
    package_size = int(Variable.get('package_size'))
    print(f"Package size: {package_size}")

    # Iterate over priority file and extract each entity
    for temp_item in priority:
        if temp_item['to_load']:
            url = temp_item['url']
            table_name = temp_item['table_name']
            print("-" * 50)
            print(f'Table name: {table_name}')
            print("-" * 50)

            etl_json = {
                "data": None,
                "logs": None,
                "meta": {
                    "data_package_size": package_size,
                    "job_uid": job_uid,
                    "table_name": table_name
                }
            }

            if table_name in ('pro_players_heroes', 'heroes_matchups'):
                # If the table name matches the one in the list below, then it contains a parameter in query string,
                # thus we need to extract the arguments for these parameters first
                if table_name == 'pro_players_heroes':
                    ids_url = 'https://api.opendota.com/api/proPlayers'
                else:
                    ids_url = 'https://api.opendota.com/api/heroes'

                print(f"Getting ids for extracting {table_name}")

                ids_data = request_data(ids_url)
                data = []
                count = 1
                for id_data in ids_data:
                    print(f'Processing {count} of total {len(ids_data)}')
                    if table_name == 'pro_players_heroes':
                        item_id = id_data['account_id']
                        temp_data = request_data(url.replace('<id>', str(item_id)))
                        result = []
                        for temp_item in temp_data:
                            temp_dict = deepcopy(temp_item)
                            temp_dict['account_id'] = item_id
                            result.append(temp_dict)
                    else:
                        item_id = id_data['id']
                        result = request_data(url.replace('<id>', str(item_id)))
                        if table_name == 'heroes_matchups':
                            for hero_matchup in result:
                                hero_matchup['hero_against_id'] = item_id
                                # list(map(lambda x: x.update({'hero_against_id': item_id}), result))
                    data.extend(result)
                    time.sleep(1)
                    count += 1
                # Insert package to service.packages
                query_insert_package(etl_json, data)
            else:
                if table_name == 'teams':
                    # Request that has a pagination
                    page = 0
                    while page is not None:
                        print(f'Page number is {page}')
                        etl_json_page = deepcopy(etl_json)
                        page_url = f'{url}?page={page}'
                        page_data = request_data(page_url)
                        # If data is not empty, insert page to service.packages, break when final package is reached
                        if len(page_data) > 0:
                            query_insert_package(etl_json_page, page_data)
                            page += 1
                        else:
                            print('Final page is reached!')
                            page = None
                elif table_name in ('pro_matches', 'picks_bans'):
                    sql_url = 'https://api.opendota.com/api/explorer?sql='
                    start_time = datetime.now() - relativedelta(years=2)
                    while start_time <= datetime.now():
                        end_time = start_time + relativedelta(days=10)
                        if table_name == 'pro_matches':
                            url_query = 'select match_id, duration, start_time, radiant_team_id, dire_team_id, ' + \
                            'leagueid, series_type, radiant_score, dire_score, radiant_win, first_blood_time ' + \
                            'from matches where leagueid is not null ' + \
                            f'and to_timestamp(start_time) between \'{start_time}\' and \'{end_time}\''
                        else:
                            url_query = 'select pb.* from picks_bans pb join matches m on pb.match_id = m.match_id ' + \
                                         f'where to_timestamp(m.start_time) between \'{start_time}\' and \'{end_time}\''
                        # sql parameter takes SQL encoded string as an input
                        url_encoded = url_query.replace(' ', '%20')
                        start_time = end_time
                        matches_data = request_data(sql_url + url_encoded)['rows']
                        query_insert_package(etl_json, matches_data)
                else:
                    data = request_data(url)
                    query_insert_package(etl_json, data)

            print(f"Successfully processed {table_name}")


def insert_data(ti):
    # Get job uid
    job_uid = ti.xcom_pull(task_ids='initialize_job', key='job_uid')
    priority = read_file('priority_path')

    for priority_item in priority:
        table_name = priority_item['table_name']

        print("-" * 50)
        print(f'Table name: {table_name}')
        print("-" * 50)

        # Get all packages' uids that correspond to this job and table name
        query = f"select jsonb_agg(p.uid) from service.packages p " \
                f"left join service.jobs j on p.job_id = j.id " \
                f"where j.uid = '{job_uid}' and p.table_name = '{table_name}';"

        package_uids = execute_query(query)[0][0]
        print(f'Package uids: {package_uids}')

        # Iterate over all package uids and insert them
        count = 1
        for package_uid in package_uids:
            print("-" * 50)
            print(f'Processing {count} of total {len(package_uids)}')
            print("-" * 50)
            query = f"call service.insert_data('{table_name}', '{package_uid}');"
            print(f'Query: {query}')
            execute_query(query, False)

            count += 1

        print('Data was inserted successfully!')


with DAG(
        dag_id='D2_ETL',
        default_args=default_args,
        start_date=datetime.now(),
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

    insert_data = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data
    )

    initialize_job >> extract_data >> insert_data
