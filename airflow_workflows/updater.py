import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from mys3utils.tools import get_jsons_from_stream, split_record

from localutils import add_to_db, download_and_store, generate_object_list
from localutils import get_prefix_from_template as get_prefix
from localutils import list_directory, print_db_stats, setup_daos


def update_last(**kwargs):
    prefix = get_prefix(**kwargs)
    target_dir = os.path.join(Variable.get('target_dir'), prefix)
    logging.info(f'Will be processing [{ target_dir }]')

    flist = list_directory(target_dir)
    logging.info(f'Files detected: { len(flist)}')

    previous_run = kwargs['prev_execution_date']
    logging.info(f'Previous run was @{previous_run}')

    station_dao, series_dao, mes_dao = setup_daos()
    m = 0

    for fname in flist:
        logging.info(f'Analyzing { fname}')
        if datetime.fromtimestamp(os.path.getmtime(fname)) < previous_run:
            logging.info('<< This file is old. Skipping')
            continue
        else:
            logging.info(
                f'>> This file is new {datetime.fromtimestamp(os.path.getmtime(fname))}')

        with open(fname, 'rb') as f:
            for record in get_jsons_from_stream(stream=f, object_name=fname):
                station, measurement, _ = split_record(record)
                m += 1
                add_to_db(station_dao, series_dao, mes_dao, station=station,
                          measurement=measurement)

    logging.info(f'Number of measurements added to DB: {m}')
    print_db_stats(station_dao, series_dao, mes_dao)
    return True


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 15, 8, 0, 0),
    # 'end_date': datetime(2013, 11, 29),
    'provide_context': True,
    'catchup': True
}

op_kwargs = {
    'base_dir': '/tmp/openaq/lists/'
}

dag = DAG('updater', default_args=default_args,
          schedule_interval=timedelta(minutes=10))

get_objects_task = PythonOperator(task_id='get_object_list2',
                                  python_callable=generate_object_list,
                                  op_kwargs=op_kwargs,
                                  dag=dag)


download_task = PythonOperator(task_id='refresh2',
                               python_callable=download_and_store,
                               op_kwargs=op_kwargs,
                               dag=dag)

db_task = PythonOperator(task_id='update2',
                         python_callable=update_last,
                         op_kwargs=op_kwargs,
                         dag=dag)

get_objects_task >> download_task >> db_task
