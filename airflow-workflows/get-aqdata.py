from airflow import DAG, utils
from airflow.operators.python_operator import PythonOperator

from datetime import timedelta
import os

from mys3utils.tools import get_object_list, FETCHES_BUCKET, serialize_object, read_object_list


def __generate_fname(suffix, **kwargs):
    base_dir = kwargs['base_dir']
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%dT%H-%M')

    fname = os.path.join(base_dir, execution_date)
    os.makedirs(fname)
    fname = os.path.join(fname, suffix)
    return fname


def get_prefixes(**kwargs):
    prefix = kwargs['prefix']
    fname = __generate_fname('prefix.dat')

    _, prefixes = get_object_list(bucket_name=FETCHES_BUCKET, prefix=prefix)

    with open(fname, 'w+') as f:
        f.write("\n".join(prefixes))


def generate_objects(**kwargs):
    fname = __generate_fname('prefix.dat')
    output = __generate_fname('objects.csv')

    with open(fname, 'r') as f:
        with open(output, 'w+') as out:
            for prefix in f:
                objects, _ = get_object_list(bucket_name=FETCHES_BUCKET, prefix=prefix)
                for obj in objects:
                    out.write(serialize_object(obj))


def add_to_database(**kwargs):
    objs = __generate_fname('objects.csv')
    with open(objs, 'r') as f:
        wl = read_object_list(f)

    for record in wl:
        print('Requesting %s ' % record)


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
