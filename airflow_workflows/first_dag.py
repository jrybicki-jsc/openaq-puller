import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def myfunc(*args, **kwargs):
    owner = default_args['owner']
    logging.info(f'Data Science is hard Mr. {owner}')
    return 'Fine'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 1, 1),
    #'end_date': datetime(2017, 1, 2),
    'provide_context': True,
    'catchup': True
}

dag = DAG(dag_id='the-first', default_args=default_args, schedule_interval='@once')

with dag:
    print_date_task = BashOperator(task_id='print_date', bash_command='date')
    ds_task = PythonOperator(task_id='do_some_python', python_callable=myfunc)
    sink = DummyOperator(task_id='sink')

print_date_task >> ds_task

for i in range(3):
    dt = DummyOperator(task_id=f'dummy_{i}', dag=dag)
    ds_task >> dt
    dt >> sink
