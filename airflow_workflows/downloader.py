from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from localutils import download_and_store, generate_object_list

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
