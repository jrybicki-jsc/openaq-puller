import logging
from datetime import timedelta

from airflow import DAG, utils
from airflow.operators.python_operator import PythonOperator
from mys3utils.tools import (FETCHES_BUCKET,
                             get_jsons_from_object, get_object_list,
                             read_object_list, serialize_object, split_record)

from localutils import add_to_db, generate_fname, print_db_stats, setup_daos


def get_prefixes(**kwargs):
    prefix = kwargs['prefix']
    fname = generate_fname('prefix.dat', **kwargs)

    _, prefixes = get_object_list(bucket_name=FETCHES_BUCKET, prefix=prefix)

    with open(fname, 'w+') as f:
        f.write("\n".join(prefixes))

    kwargs['ti'].xcom_push(key='prefixes_location', value=fname)


def generate_objects(**kwargs):
    fname = kwargs['ti'].xcom_pull(
        key='prefixes_location', task_ids='get_prefixes')
    output = generate_fname(
        suffix='objects.csv', base_dir=kwargs['base_dir'], execution_date=kwargs['execution_date'].strftime('%Y-%m-%dT%H-%M'))

    logging.info(f'The task will read from {fname} and write to: {output}')

    with open(fname, 'r') as f:
        with open(output, 'w+') as out:
            for prefix in f:
                objects, _ = get_object_list(
                    bucket_name=FETCHES_BUCKET, prefix=prefix)
                for obj in objects:
                    out.write(serialize_object(obj))

    kwargs['ti'].xcom_push(key='object_location', value=output)


def add_to_database(**kwargs):
    objs = kwargs['ti'].xcom_pull(
        key='object_location', task_ids='generate_object_list')
    logging.info(f'Processing object list from {objs}')
    with open(objs, 'r') as f:
        wl = read_object_list(f)

    # execution_date = kwargs['execution_date'].strftime('%Y-%m-%dT%H-%M')
    # previous_run = kwargs['prev_execution_date'].strftime('%Y-%m-%dT%H-%M')
    # filtered = list(filter_objects(all_objects=wl, start_date=previous_run, end_date=execution_date))
    filtered = list(wl)

    station_dao, series_dao, mes_dao = setup_daos()
    records = 0

    for obj in filtered:
        for record in get_jsons_from_object(bucket=FETCHES_BUCKET, object_name=obj['Name']):
            station, measurement, _ = split_record(record)
            add_to_db(station_dao=station_dao, series_dao=series_dao,
                      mes_dao=mes_dao, station=station, measurement=measurement)
            records += 1

    print_db_stats(station_dao, series_dao, mes_dao)


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

dag = DAG('get-aqdata', default_args=default_args,
          schedule_interval=timedelta(days=1))

get_prefixes_task = PythonOperator(task_id='get_prefixes', python_callable=get_prefixes,
                                   op_kwargs=op_kwargs, dag=dag)

generate_object_list_task = PythonOperator(task_id='generate_object_list', python_callable=generate_objects,
                                           op_kwargs=op_kwargs, dag=dag)

add_to_database_task = PythonOperator(task_id='add_to_db', python_callable=add_to_database,
                                      op_kwargs=op_kwargs, dag=dag)

get_prefixes_task >> generate_object_list_task >> add_to_database_task
