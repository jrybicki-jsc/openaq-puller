import glob
import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from mys3utils.tools import get_jsons_from_stream, split_record

from localutils import add_to_db
from localutils import get_prefix_from_template as get_prefix
from localutils import print_db_stats, setup_daos


def go_through(**kwargs):
    prefix = get_prefix(**kwargs)
    target_dir = os.path.join(Variable.get('target_dir'), prefix)
    logging.info(f'Will be processing [{ target_dir }]')

    flist = glob.glob(os.path.join(target_dir, '*'))
    logging.info(f'Files detected: { len(flist)}')

    station_dao, series_dao, mes_dao = setup_daos()

    for fname in flist:
        logging.info(f'Processing { fname}')
        with open(fname, 'rb') as f:
            for record in get_jsons_from_stream(stream=f, object_name=fname):
                station, measurement, _ = split_record(record)
                add_to_db(station_dao, series_dao, mes_dao, station=station,
                          measurement=measurement)

    print_db_stats(station_dao, series_dao, mes_dao)
    return True


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2013, 11, 26),
    'end_date': datetime(2013, 12, 28),
    'provide_context': True,
    'catchup': True
}

# the task is to be run after the download workflow

dag = DAG(dag_id='local-processor', default_args=default_args,
          schedule_interval='@daily')
with dag:
    go_through_files = PythonOperator(task_id='go_through_files',
                                      python_callable=go_through,
                                      dag=dag)
