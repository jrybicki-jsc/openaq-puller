import concurrent.futures

import logging

from datetime import timedelta, datetime
from string import Template
from airflow.hooks.postgres_hook import PostgresHook
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


from mys3utils.tools import get_jsons_from_object, FETCHES_BUCKET, split_record
from localutils import get_file_list, setup_daos, add_to_db, print_db_stats


def local_process_file(object_name):
    for record in get_jsons_from_object(bucket=FETCHES_BUCKET, object_name=object_name):
        station, measurement, _ = split_record(record)

        yield [station, measurement]

def setup_objectlist(**kwargs):
    date = kwargs['execution_date']
    prefix = Template(kwargs['prefix-pattern']).substitute(date=date.strftime('%Y-%m-%d'))
    pfl = get_file_list(prefix=prefix, **kwargs)
    return pfl


def generate_object_list(**kwargs):
    pfl = setup_objectlist(**kwargs)
    logging.info(f"Retrieving object names for prefix: { pfl.get_prefix() }")
    pfl.update()
    pfl.store()

def transform_objects(**kwargs):
    pfl = setup_objectlist(**kwargs)
    pfl.load()
    objects_count = len(pfl.get_list())
    logging.info(f'Loaded {objects_count} objects.')

    station_dao, series_dao, mes_dao = setup_daos()
    process = lambda x: local_process_file(x['Name'])

    with concurrent.futures.ThreadPoolExecutor(max_workers=None) as executor:
        for obj, results in zip(pfl.get_list(), executor.map(process, pfl.get_list())):
            logging.info(f"Processing { obj['Name'] } ({ obj['Size']})")
            # we are linerizing it here anyways?
            for it in results:
                add_to_db(station_dao, series_dao, mes_dao, station=it[0], measurement=it[1])

    print_db_stats(station_dao, series_dao, mes_dao)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 7, 29),
    'end_date': datetime(2018, 7, 31),
    'provide_context': True,
    'catchup': True
}

op_kwargs = {
    'prefix-pattern': 'realtime/$date/',
    'base_dir': '/tmp/'
}

dag = DAG('prefix-oriented', default_args=default_args, schedule_interval=timedelta(1))

get_objects_task = PythonOperator(task_id='get_object_list',
                                  python_callable=generate_object_list,
                                  op_kwargs=op_kwargs,
                                  dag=dag)

transform_and_store_task = PythonOperator(task_id='transform',
                                          python_callable=transform_objects,
                                          op_kwargs=op_kwargs,
                                          dag=dag)

get_objects_task >> transform_and_store_task
