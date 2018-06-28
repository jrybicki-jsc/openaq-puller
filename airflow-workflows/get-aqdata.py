from airflow import DAG, utils
from airflow.operators.python_operator import PythonOperator

from datetime import timedelta
import os
import logging

from models.Measurement import MeasurementDAO
from models.StationMeta import get_engine, StationMetaCoreDAO
from mys3utils.tools import get_object_list, FETCHES_BUCKET, serialize_object, read_object_list, split_record, \
    get_jsons_from_object, filter_objects


def __generate_fname(suffix, **kwargs):
    base_dir = kwargs['base_dir']
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%dT%H-%M')

    fname = os.path.join(base_dir, execution_date)

    os.makedirs(fname, exist_ok=True)
    fname = os.path.join(fname, suffix)
    return fname


def get_prefixes(**kwargs):
    prefix = kwargs['prefix']
    fname = __generate_fname('prefix.dat', **kwargs)

    _, prefixes = get_object_list(bucket_name=FETCHES_BUCKET, prefix=prefix)

    with open(fname, 'w+') as f:
        f.write("\n".join(prefixes))

    kwargs['ti'].xcom_push(key='prefixes_location', value=fname)


def generate_objects(**kwargs):
    fname = kwargs['ti'].xcom_pull(key='prefixes_location', task_ids='get_prefixes')
    output = __generate_fname('objects.csv', **kwargs)
    logging.info('The task will read from %s and write to: %s' % (fname, output))

    with open(fname, 'r') as f:
        with open(output, 'w+') as out:
            for prefix in f:
                objects, _ = get_object_list(bucket_name=FETCHES_BUCKET, prefix=prefix)
                for obj in objects:
                    out.write(serialize_object(obj))

    kwargs['ti'].xcom_push(key='object_location', value=output)


def add_to_database(**kwargs):
    objs = kwargs['ti'].xcom_pull(key='object_location', task_ids='generate_object_list')
    logging.info('Processing object list from %s' % objs)
    with open(objs, 'r') as f:
        wl = read_object_list(f)

    #execution_date = kwargs['execution_date'].strftime('%Y-%m-%dT%H-%M')
    #previous_run = kwargs['prev_execution_date'].strftime('%Y-%m-%dT%H-%M')
    #filtered = list(filter_objects(all_objects=wl, start_date=previous_run, end_date=execution_date))
    filtered = list(wl)

    engine = get_engine()
    station_dao = StationMetaCoreDAO(engine=engine)
    mes_dao = MeasurementDAO(engine=engine)
    station_dao.create_table()

    records = 0

    for obj in filtered:
        for record in get_jsons_from_object(bucket=FETCHES_BUCKET, object_name=obj['Name']):
            station, measurement, ext = split_record(record)

            stat_id = station_dao.store_from_json(station)

            mes_dao.store(station_id=stat_id, parameter=measurement['parameter'],
                          value=measurement['value'], unit=measurement['unit'],
                          averagingPeriod=measurement['averagingPeriod'],
                          date=measurement['date']['utc'])

            records += 1

    logging.info('%d stations stored in db\n', len(station_dao.get_all()))
    logging.info('%d measurements stored in db\n', len(mes_dao.get_all()))
    logging.info('%d valid records processed\n', records)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': utils.dates.days_ago(2),
    'provide_context': True
}

op_kwargs = {
    'base_dir': '/tmp/data/',
    'prefix': 'test-realtime/',
}

dag = DAG('get-aqdata', default_args=default_args, schedule_interval=timedelta(1))

get_prefixes_task = PythonOperator(task_id='get_prefixes', python_callable=get_prefixes,
                                   op_kwargs=op_kwargs, dag=dag)

generate_object_list_task = PythonOperator(task_id='generate_object_list', python_callable=generate_objects,
                                           op_kwargs=op_kwargs, dag=dag)

add_to_database_task = PythonOperator(task_id='add_to_db', python_callable=add_to_database,
                                      op_kwargs=op_kwargs, dag=dag)

get_prefixes_task >> generate_object_list_task >> add_to_database_task
