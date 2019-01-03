import concurrent.futures
import logging
import os
from datetime import datetime, timedelta
from string import Template

import boto3
import botocore
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from mys3utils.tools import FETCHES_BUCKET

from localutils import get_file_list, get_prefix_from_template as get_prefix


def generate_object_list(*args, **kwargs):
    prefix = get_prefix(**kwargs)
    logging.info(f'Will be getting objects for {prefix}')
    pfl = get_file_list(prefix=prefix, **kwargs)
    pfl.update()


def download_and_store(**kwargs):
    prefix = get_prefix(**kwargs)
    target_dir = os.path.join(Variable.get('target_dir'), prefix)
    os.makedirs(target_dir, exist_ok=True)

    pfl = get_file_list(prefix=prefix, **kwargs)
    pfl.load()
    logging.info(f"Downloading {len(pfl.get_list())} objects from {prefix} to {target_dir}")

    client = boto3.client('s3', config=botocore.client.Config(
        signature_version=botocore.UNSIGNED))

    def myfunc(obj, client=client):
        if obj['Name'].endswith('/'):
            return 'skipped'
        local_name = os.path.join(target_dir, obj['Name'].split('/')[-1])
        client.download_file(Bucket=FETCHES_BUCKET,
                             Key=obj['Name'], Filename=local_name)
        return 'Done'

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for obj, status in zip(pfl.get_list(), executor.map(myfunc, pfl.get_list())):
            logging.info(f"{ obj['Name']} status: { status }")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2013, 11, 26),
    'end_date': datetime(2013, 11, 29),
    'provide_context': True,
    'catchup': True
}

op_kwargs = {
    #    'prefix-pattern': 'test-realtime-gzip/$date/',
    'base_dir': '/tmp/openaq/lists/'
}

dag = DAG('downloader', default_args=default_args,
          schedule_interval=timedelta(1))

get_objects_task = PythonOperator(task_id='get_object_list',
                                  python_callable=generate_object_list,
                                  op_kwargs=op_kwargs,
                                  dag=dag)

download_task = PythonOperator(task_id='download',
                               python_callable=download_and_store,
                               op_kwargs=op_kwargs,
                               dag=dag)


get_objects_task >> download_task
