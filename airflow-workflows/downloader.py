import os
import boto3
import botocore

import concurrent.futures
from datetime import timedelta, datetime
from string import Template
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging
from mys3utils.tools import FETCHES_BUCKET
from localutils import get_file_list

from airflow.models import Variable

def get_prefix(**kwargs):
    date = kwargs['execution_date']
    prefix_pattern = Variable.get('prefix_pattern')
    if prefix_pattern is None:
        logging.info('No prefix pattern provided (use prefix_pattern variable)')
        prefix_pattern = 'test-realtime-gzip/$date/'
    prefix = Template(prefix_pattern).substitute(date=date.strftime('%Y-%m-%d'))
    return prefix



def generate_object_list(*args, **kwargs):
    prefix = get_prefix(**kwargs)
    logging.info('Will be getting objects for %s', prefix)
    pfl = get_file_list(prefix=prefix, **kwargs)
    pfl.update()
    pfl.store()


def download_and_store(**kwargs):
    prefix = get_prefix(**kwargs)

    target_dir = os.path.join(Variable.get('target_dir'), prefix)
    os.makedirs(target_dir, exist_ok=True)

    pfl = get_file_list(prefix=prefix, **kwargs)
    pfl.load()
    objects_count = len(pfl.get_list())
    logging.info('Downloading %d objects from %s to %s',  objects_count, prefix, target_dir)
    
    client = boto3.client('s3', config=botocore.client.Config(signature_version=botocore.UNSIGNED))

    def myfunc(obj, client=client):
        if obj['Name'].endswith('/'):
            return 'skipped'
        local_name = os.path.join(target_dir, obj['Name'].split('/')[-1])
        client.download_file(Bucket=FETCHES_BUCKET, Key=obj['Name'], Filename=local_name)
        return 'Done'

    with concurrent.futures.ThreadPoolExecutor() as execturor:
        for obj, status in zip(pfl.get_list(), execturor.map(myfunc, pfl.get_list())):
            logging.info('%s status: %s', obj['Name'], status)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 9, 27),
    'end_date': datetime(2017, 9, 29),
    'provide_context': True,
    'catchup': True
}

op_kwargs = {
#    'prefix-pattern': 'test-realtime-gzip/$date/',
    'base_dir': '/tmp/'
}

dag = DAG('downloader', default_args=default_args, schedule_interval=timedelta(1))

get_objects_task = PythonOperator(task_id='get_object_list',
                                  python_callable=generate_object_list,
                                  op_kwargs=op_kwargs,
                                  dag=dag)

download_task = PythonOperator(task_id='download',
                               python_callable=download_and_store,
                               op_kwargs=op_kwargs,
                               dag=dag)


get_objects_task >> download_task