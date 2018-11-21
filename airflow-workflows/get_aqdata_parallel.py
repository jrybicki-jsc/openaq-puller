import logging
import os

from datetime import timedelta, datetime
import concurrent.futures

from airflow import DAG, utils
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

from models.Measurement import MeasurementDAO
from models.StationMeta import get_engine, StationMetaCoreDAO
from mys3utils.tools import get_object_list, FETCHES_BUCKET, serialize_object, read_object_list, get_objects, \
    filter_objects, filter_prefixes

from localutils import generate_fname, local_process_file, add_to_db


def store_prefix_list(prefixes, **kwargs):
    base_dir = kwargs['base_dir']
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')

    fname = generate_fname(suffix='prefix.dat', base_dir=base_dir, execution_date=execution_date)
    logging.info('Storing %d prefixes in %s', len(prefixes), fname)
    with open(fname, 'w+') as f:
        f.write("\n".join(prefixes))

    return fname


def new_prefix_list_needed(**kwargs):
    execution_date = kwargs['execution_date'].date()
    previous_run = kwargs['prev_execution_date'].date()
    logging.info('Start: %s End: %s', previous_run, execution_date)
    td = timedelta(days=1)

    if execution_date - previous_run >= td:
        logging.info('The prefix generation task is too far back. Regenerating')
        return 'generate_prefix_list'

    location = generate_fname(suffix='prefix.dat',
                              base_dir=kwargs['base_dir'],
                              execution_date=execution_date.strftime('%Y-%m-%d'))

    if not os.path.isfile(location):
        logging.info('Old prefix list does not exist for some reason. Regenerating')
        return 'generate_prefix_list'

    kwargs['ti'].xcom_push(key='prefixes_location', value=location)
    return 'dummy'


def generate_prefix_list(**kwargs):
    logging.info('Generating prefix list')
    prefix = kwargs['prefix']

    _, prefixes = get_object_list(bucket_name=FETCHES_BUCKET, prefix=prefix)
    location = store_prefix_list(prefixes, **kwargs)

    kwargs['ti'].xcom_push(key='prefixes_location', value=location)


def generate_object_list_parallel(**kwargs):
    fname = kwargs['ti'].xcom_pull(key='prefixes_location', task_ids='generate_prefix_list')
    output = generate_fname(suffix='objects.csv',
                            base_dir=kwargs['base_dir'],
                            execution_date=kwargs['execution_date'].strftime('%Y-%m-%dT%H-%M'))
    logging.info('The task will read from %s and write to: %s' % (fname, output))

    with open(fname, 'r') as f:
        prefix_list = list(map(lambda a: a.strip(), f.readlines()))

    if kwargs['filter_prefixes']:
        logging.info('Filtering prefixes')
        prefix_list = list(filter_prefixes(prefixes=prefix_list,
                                           start_date=kwargs['execution_date']-timedelta(days=1),
                                           end_date=kwargs['execution_date']))

    logging.info('Number of prefixes to process: %d ', len(prefix_list))

    with concurrent.futures.ThreadPoolExecutor(max_workers=None) as executor:
        future_objects = {executor.submit(get_objects, prefix): prefix for prefix in prefix_list}
        with open(output, 'w') as f:
            for future in concurrent.futures.as_completed(future_objects):
                url = future_objects[future]
                try:
                    data = future.result()
                except Exception as exc:
                    logging.error('%r generated an exception: %s' % (url, exc))
                else:
                    for o in data:
                        f.write(serialize_object(o))

    kwargs['ti'].xcom_push(key='object_location', value=output)


def store_objects_in_db(**kwargs):
    objs = kwargs['ti'].xcom_pull(key='object_location', task_ids='generate_object_list')
    logging.info('Processing object list from %s', objs)
    with open(objs, 'r') as f:
        wl = read_object_list(f)

    execution_date = kwargs['execution_date']
    previous_run = kwargs['prev_execution_date']

    if kwargs['filter_objects']:
        logging.info('Filtering objects...')
        filtered = list(filter_objects(all_objects=wl, start_date=previous_run, end_date=execution_date))
        logging.info('Filterd objects. Number of objects from [%s, %s]: %d', previous_run, execution_date,
                     len(filtered))
    else:
        filtered = list(wl)
        logging.info('Number of non-filtered objects %d', len(filtered))

    engine = get_engine()
    station_dao = StationMetaCoreDAO(engine=engine)
    mes_dao = MeasurementDAO(engine=engine)
    station_dao.create_table()

    with concurrent.futures.ThreadPoolExecutor(max_workers=None) as executor:
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


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2013, 11, 26),  # datetime(2017, 9, 26)
    'end_date': datetime(2013, 11, 30),  # datetime(2017, 9, 29)
    'concurrency': 4,  # how many running task instances are allowed
    'provide_context': True,
    'catchup': True,
    'trigger_rule': 'all_done',
}

op_kwargs = {
    'base_dir': '/tmp/data/',
    'prefix': 'realtime/',  # 'test-realtime/',
    'filter_prefixes': True,
    'filter_objects': False,
}

dag = DAG('get-aqdata-parallel', default_args=default_args, schedule_interval=timedelta(days=1))

branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=new_prefix_list_needed,
    dag=dag)

dummy_task_wrapper = DummyOperator(task_id='dummy', dag=dag)

get_prefixes_task = PythonOperator(task_id='generate_prefix_list',
                                   python_callable=generate_prefix_list,
                                   op_kwargs=op_kwargs,
                                   dag=dag)

generate_object_list_task = PythonOperator(task_id='generate_object_list',
                                           python_callable=generate_object_list_parallel,
                                           op_kwargs=op_kwargs,
                                           dag=dag)

add_to_database_task = PythonOperator(task_id='store_objects_in_db',
                                      python_callable=store_objects_in_db,
                                      op_kwargs=op_kwargs,
                                      dag=dag)

branch_task >> dummy_task_wrapper >> generate_object_list_task
branch_task >> get_prefixes_task

get_prefixes_task >> generate_object_list_task >> add_to_database_task
