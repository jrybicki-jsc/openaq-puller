from airflow import DAG, utils
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

import logging
from datetime import timedelta, datetime


def myfunc(*args, **kwargs):
    owner = default_args['owner']
    logging.info('Data Science is hard Mr. %s', owner)
    return 'Fine'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 1, 1),
    'end_date': datetime(2017, 1, 2), 
    'provide_context': True,
    'catchup': True
}

dag = DAG('the-first', default_args=default_args, schedule_interval='@daily')

with dag:
    print_date_task = BashOperator(task_id='print_date', bash_command='date')
    ds_task = PythonOperator(task_id='do_some_python', python_callable=myfunc)
    sink = DummyOperator(task_id='sink')

print_date_task >> ds_task 

for i in range(3):
    dt = DummyOperator(task_id='dummy_%d'%i, dag=dag)
    ds_task >> dt
    dt >> sink
    




