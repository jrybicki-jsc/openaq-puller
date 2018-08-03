import concurrent.futures
from datetime import timedelta, datetime
import types
from string import Template

from airflow.hooks.postgres_hook import PostgresHook
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging

from models.Measurement import MeasurementDAO
from models.StationMeta import StationMetaCoreDAO
from mys3utils.object_list import FileBasedObjectList
from mys3utils.tools import get_jsons_from_object, FETCHES_BUCKET, split_record


def generate_object_list(**kwargs):
    date = kwargs['execution_date']
    prefix = Template(kwargs['prefix-pattern']).substitute(date=date.strftime('%Y-%m-%d'))
    logging.info('Will be getting objects for %s', prefix)
    pfl = FileBasedObjectList(prefix=prefix, **kwargs)
    pfl.retrieve()
    pfl.store()


def local_process_file(object_name):
    for record in get_jsons_from_object(bucket=FETCHES_BUCKET, object_name=object_name):
        station, measurement, ext = split_record(record)

        yield [station, measurement]


def add_to_db(station_dao, mes_dao, station, measurement):
    stat_id = station_dao.store_from_json(station)
    mes_dao.store(station_id=stat_id,
                  parameter=measurement['parameter'],
                  value=measurement['value'],
                  unit=measurement['unit'],
                  averagingPeriod=measurement['averagingPeriod'],
                  date=measurement['date']['utc'])


def transform_objects(**kwargs):
    date = kwargs['execution_date']
    prefix = Template(kwargs['prefix-pattern']).substitute(date=date.strftime('%Y-%m-%d'))

    pg = PostgresHook(postgres_conn_id='openaq-db')
    wrapper = types.SimpleNamespace()
    wrapper.get_engine = pg.get_sqlalchemy_engine
    station_dao = StationMetaCoreDAO(engine=wrapper)
    mes_dao = MeasurementDAO(engine=wrapper)
    station_dao.create_table()

    pfl = FileBasedObjectList(prefix=prefix, **kwargs)
    pfl.load()
    objects_count = len(pfl.get_list())

    if objects_count / 16 > 3:
        workers = 16
    elif objects_count > 1:
        workers = objects_count
    else:
        workers = 1

    logging.info('Loaded %d objects. Will be using %d workers', objects_count, workers)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        processor_objects = {executor.submit(local_process_file, obj['Name']): obj['Name'] for obj in pfl.get_list()}

        for future in concurrent.futures.as_completed(processor_objects):
            object_name = processor_objects[future]
            try:
                rr = future.result()
            except Exception as exc:
                logging.warning('%r generated an exception: %s', object_name, exc)
            else:
                logging.info('Processing %s', object_name)
                for it in rr:
                    add_to_db(station_dao=station_dao, mes_dao=mes_dao, station=it[0], measurement=it[1])

    logging.info('%d stations stored in db\n', len(station_dao.get_all()))
    logging.info('%d measurements stored in db\n', len(mes_dao.get_all()))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2013, 11, 26),
    'end_date': datetime(2013, 11, 28),
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
