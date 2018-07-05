import logging
import os
from datetime import timedelta
import concurrent.futures

from airflow import DAG, utils
from airflow.operators.python_operator import PythonOperator

from models.Measurement import MeasurementDAO
from models.StationMeta import get_engine, StationMetaCoreDAO
from mys3utils.tools import get_object_list, FETCHES_BUCKET, serialize_object, read_object_list, get_objects, \
    get_jsons_from_object, split_record, filter_objects


def generate_fname(suffix, **kwargs):
    base_dir = kwargs['base_dir']
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%dT%H-%M')

    fname = os.path.join(base_dir, execution_date)

    os.makedirs(fname, exist_ok=True)
    fname = os.path.join(fname, suffix)
    return fname


def get_prefixes(**kwargs):
    prefix = kwargs['prefix']
    fname = generate_fname('prefix.dat', **kwargs)

    _, prefixes = get_object_list(bucket_name=FETCHES_BUCKET, prefix=prefix)

    with open(fname, 'w+') as f:
        f.write("\n".join(prefixes))

    kwargs['ti'].xcom_push(key='prefixes_location', value=fname)


def generate_object_list_parallel(**kwargs):
    fname = kwargs['ti'].xcom_pull(key='prefixes_location', task_ids='get_prefixes')
    output = generate_fname('objects.csv', **kwargs)
    logging.info('The task will read from %s and write to: %s' % (fname, output))

    with open(fname, 'r') as f:
        ins = map(lambda a: a.strip(), f.readlines())

    execution_date = kwargs['execution_date'].strftime('%Y-%m-%dT%H-%M')
    previous_run = kwargs['prev_execution_date'].strftime('%Y-%m-%dT%H-%M')
    prefix_list = list(filter_objects(all_objects=ins, start_date=previous_run, end_date=execution_date))

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        future_to_url = {executor.submit(get_objects, prefix): prefix for prefix in prefix_list}
        with open(output, 'w') as f:
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    data = future.result()
                except Exception as exc:
                    logging.error('%r generated an exception: %s' % (url, exc))
                else:
                    for o in data:
                        f.write(serialize_object(o))

    kwargs['ti'].xcom_push(key='object_location', value=output)


def add_to_db(station_dao, mes_dao, station, measurement):
    stat_id = station_dao.store_from_json(station)
    mes_dao.store(station_id=stat_id,
                  parameter=measurement['parameter'],
                  value=measurement['value'],
                  unit=measurement['unit'],
                  averagingPeriod=measurement['averagingPeriod'],
                  date=measurement['date']['utc'])


def local_process_file(object_name):
    ret = []
    for record in get_jsons_from_object(bucket=FETCHES_BUCKET, object_name=object_name):
        station, measurement, ext = split_record(record)

        ret.append([station, measurement])

    return ret


def add_to_database(**kwargs):
    objs = kwargs['ti'].xcom_pull(key='object_location', task_ids='generate_object_list')
    logging.info('Processing object list from %s', objs)
    with open(objs, 'r') as f:
        wl = read_object_list(f)

    engine = get_engine()
    station_dao = StationMetaCoreDAO(engine=engine)
    mes_dao = MeasurementDAO(engine=engine)
    station_dao.create_table()

    filtered = list(wl)

    records = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        processor_objects = {executor.submit(local_process_file, obj['Name']): obj['Name'] for obj in filtered}

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

dag = DAG('get-aqdata-parallel', default_args=default_args, schedule_interval=timedelta(1))

get_prefixes_task = PythonOperator(task_id='get_prefixes',
                                   python_callable=get_prefixes,
                                   op_kwargs=op_kwargs,
                                   dag=dag)

generate_object_list_task = PythonOperator(task_id='generate_object_list',
                                           python_callable=generate_object_list_parallel,
                                           op_kwargs=op_kwargs,
                                           dag=dag)

add_to_database_task = PythonOperator(task_id='add_to_db',
                                      python_callable=add_to_database,
                                      op_kwargs=op_kwargs,
                                      dag=dag)

get_prefixes_task >> generate_object_list_task >> add_to_database_task
