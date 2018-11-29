import glob
import logging
import os
from datetime import datetime, timedelta
from string import Template

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from mys3utils.tools import get_jsons_from_stream, split_record

from prefix_oriented import add_to_db, setup_daos


def get_prefix(**kwargs):
    date = kwargs['execution_date']
    prefix_pattern = Variable.get('prefix_pattern')
    if prefix_pattern is None:
        logging.warning(
            'No prefix pattern provided (use prefix_pattern variable)')
        prefix_pattern = 'test-realtime-gzip/$date/'
    return Template(prefix_pattern).substitute(date=date.strftime('%Y-%m-%d'))


def go_through(**kwargs):
    prefix = get_prefix(**kwargs)
    target_dir = os.path.join(Variable.get('target_dir'), prefix)

    logging.info(f'Will be processing { target_dir }')

    flist = glob.glob(os.path.join(target_dir, '*'))
    logging.info(f'Files detected: { len(flist)}')

    mes_dao, station_dao = setup_daos()

    for fname in flist:
        logging.info(f'Processing { fname}')
        with open(fname, 'rb') as f:
            for record in get_jsons_from_stream(stream=f, object_name=fname):
                station, measurement, _ = split_record(record)
                add_to_db(station_dao, mes_dao, station=station,
                          measurement=measurement)

    logging.info(f"{ len(station_dao.get_all())} stations stored in db")
    logging.info(f"{ len(mes_dao.get_all())} measurements stored in db")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 9, 27),
    'end_date': datetime(2017, 9, 29),
    'provide_context': True,
    'catchup': True
}

# the task is to be run after the download workflow

dag = DAG(dag_id='local_processor', default_args=default_args,
          schedule_interval='@daily')
with dag:
    go_through_files = PythonOperator(task_id='go_through_files',
                                      python_callable=go_through,
                                      dag=dag)
