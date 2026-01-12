import json
import requests
import time
import logging

from copy import deepcopy

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

from urllib.parse import quote_plus, quote
from dateutil.relativedelta import relativedelta

default_args = {
    "owner": "Vladlen",
    "retries": 50,
    "retry_delay": timedelta(seconds=30)
}


def read_file(variable):
    """
    Read a JSON file path from Airflow Variable and load its contents.
    
    :param variable: Airflow Variable name containing the file path
    :return: Parsed JSON content
    """
    try:
        path = Variable.get(variable)
        with open(path, 'r', encoding='utf-8') as f:
            file = json.load(f)
        return file
    except Exception as e:
        logging.error(f"Error reading file from variable '{variable}': {e}")
        raise


def execute_query(query, fetch=True):
    """
    Execute a SQL query against the DWH PostgreSQL database.
    
    :param query: SQL query string to execute
    :param fetch: Whether to fetch results (True) or just execute (False)
    :return: Query results if fetch=True, None otherwise
    """
    pg_hook = PostgresHook(
        postgres_conn_id='d2_dwh'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    try:
        cursor.execute(query)
        if fetch:
            returned_value = cursor.fetchall()
            pg_conn.commit()
            return returned_value
        else:
            pg_conn.commit()
            return None
    except Exception as e:
        pg_conn.rollback()
        logging.error(f"Error executing query: {query[:100]}... Error: {str(e)}")
        raise
    finally:
        cursor.close()
        pg_conn.close()


def initialize_job(ti):
    """
    DAG TASK
    Creates a new job in service.jobs table and pushes its uuid to the XCom
    :param ti: XCom argument
    """
    try:
        job_code = 'dota2_etl'
        logging.info("Creating a new job...")
        query = f"SELECT service.create_job('{job_code}');"
        result = execute_query(query)
        if result and len(result) > 0 and result[0][0]:
            job_uid = result[0][0]
            logging.info(f"Job has been created successfully! Job UID: {job_uid}")
            ti.xcom_push(key='job_uid', value=job_uid)
        else:
            raise ValueError("Failed to create job - no UID returned")
    except Exception as e:
        logging.error(f"ERROR creating job: {e}")
        raise


def query_insert_package(etl_json_item, data):
    """
    Creates a new package in service.packages table with the given JSON
    :param etl_json_item: header of result JSON
    :param data: data that was extracted from the source
    """
    if len(data) >= 0:
        etl_json_item['data'] = data
        
        # Convert to JSON string and properly escape for PostgreSQL
        etl_json_str = json.dumps(etl_json_item)
        # Escape single quotes for SQL (PostgreSQL jsonb type handles this, but we need to escape for string literal)
        etl_json_str_escaped = etl_json_str.replace("'", "''")
        
        # Use jsonb type casting in PostgreSQL
        query = f"SELECT service.create_package('{etl_json_str_escaped}'::jsonb);"
        execute_query(query, fetch=False)
        logging.info("Package has been created successfully!")
    else:
        logging.warning("Data is empty, skipping package creation")
        return "Data is empty!"

def request_data(url, max_retries=10, retry_delay=5):
    """
    Requests data based on the provided URL. If response is not successful, try again after a delay. 
    Raise an exception if number of tries exceeds count limit.
    
    :param url: Query string of request
    :param max_retries: Maximum number of retry attempts
    :param retry_delay: Delay in seconds between retries
    :return: JSON of the response
    """
    logging.info(f"Requesting data from: {url}")
    count = 0
    while count < max_retries:
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                logging.info("Data has been retrieved successfully!")
                return response.json()
            else:
                logging.warning(f"Response code is {response.status_code}, response message: {response.text[:200]}")
                count += 1
                if count < max_retries:
                    time.sleep(retry_delay)
                    continue
        except requests.exceptions.RequestException as e:
            logging.warning(f"Request exception: {str(e)}")
            count += 1
            if count < max_retries:
                time.sleep(retry_delay)
                continue
    
    raise ValueError(f"ERROR requesting data after {max_retries} attempts from URL: {url}")


def extract_data(ti):
    """
    DAG TASK
    Extracts data from API and inserts packages to service.packages
    :param ti: XCom argument
    """
    priority = read_file('priority_path')
    job_uid = ti.xcom_pull(task_ids='initialize_job', key='job_uid')

    # Size of package that data should be split on
    try:
        package_size = int(Variable.get('package_size'))
    except Exception:
        package_size = 1000  # Default value if variable not set
        logging.warning(f"Variable 'package_size' not set, using default: {package_size}")
    logging.info(f"Package size: {package_size}")

    # Iterate over priority file and extract each entity
    for temp_item in priority:
        if temp_item['to_load']:
            url = temp_item['url']
            table_name = temp_item['table_name']
            logging.info("-" * 50)
            logging.info(f'Table name: {table_name}')
            logging.info("-" * 50)

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

                logging.info(f"Getting ids for extracting {table_name}")

                ids_data = request_data(ids_url)
                data = []
                count = 1
                for id_data in ids_data:
                    logging.info(f'Processing {count} of total {len(ids_data)}')
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
                        logging.info(f'Page number is {page}')
                        etl_json_page = deepcopy(etl_json)
                        page_url = f'{url}?page={page}'
                        page_data = request_data(page_url)
                        # If data is not empty, insert page to service.packages, break when final package is reached
                        if len(page_data) > 0:
                            query_insert_package(etl_json_page, page_data)
                            page += 1
                        else:
                            logging.info('Final page is reached!')
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
                        # Properly encode the SQL query for URL
                        url_encoded = quote_plus(url_query)
                        start_time = end_time
                        matches_data = request_data(sql_url + url_encoded)['rows']
                        query_insert_package(etl_json, matches_data)
                else:
                    data = request_data(url)
                    query_insert_package(etl_json, data)

            logging.info(f"Successfully processed {table_name}")


def insert_data(ti):
    # Get job uid
    job_uid = ti.xcom_pull(task_ids='initialize_job', key='job_uid')
    priority = read_file('priority_path')

    for priority_item in priority:
        if priority_item['to_load']:
            table_name = priority_item['table_name']

            logging.info("-" * 50)
            logging.info(f'Table name: {table_name}')
            logging.info("-" * 50)

            # Get all packages' uids that correspond to this job and table name
            query = f"SELECT jsonb_agg(p.uid) FROM service.packages p " \
                    f"LEFT JOIN service.jobs j ON p.job_id = j.id " \
                    f"WHERE j.uid = '{job_uid}' AND p.table_name = '{table_name}';"

            result = execute_query(query)
            if not result or not result[0] or not result[0][0]:
                logging.warning(f"No packages found for table {table_name} and job {job_uid}")
                continue
                
            package_uids = result[0][0]
            logging.info(f'Found {len(package_uids)} packages for table {table_name}')

            # Iterate over all package uids and insert them
            count = 1
            for package_uid in package_uids:
                logging.info("-" * 50)
                logging.info(f'Processing package {count} of total {len(package_uids)}')
                logging.info("-" * 50)
                query = f"CALL service.insert_data('{table_name}', '{package_uid}');"
                logging.debug(f'Executing query for package {package_uid}')
                execute_query(query, fetch=False)
                count += 1

            logging.info(f'Data was inserted successfully for table {table_name}!')


with DAG(
        dag_id='D2_ETL',
        default_args=default_args,
        start_date=datetime(2024, 1, 1),  # Fixed start date instead of now() for better DAG behavior
        schedule_interval='@daily',
        catchup=False,  # Don't run backfill automatically
        tags=['dota2', 'etl', 'data-warehouse']
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
