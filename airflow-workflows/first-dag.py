from airflow import DAG, utils
from airflow.operators.python_operator import PythonOperator
#from airflow.lineage.datasets import File

from datetime import timedelta
import os


## example of a DAG which writes to a file, there is no guarantee that
## it will work in a distributed setting (workers could be working on
## different machines)

def get_objectnames(**kwargs):
    base_dir = kwargs['base_dir']
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%dT%H-%M')
    for k, v in kwargs.items():
        print('%s --> %s' %(k, v))

    print('Getting prefixes %s' % base_dir)
    fname = os.path.join(base_dir, execution_date)
    os.makedirs(fname)
    fname = os.path.join(fname, 'objects.csv')

    with open(fname, 'w+') as f:
        for i in range(10):
            f.write('Somedata %d\n' % i)

    return 'Yo'


def add_to_database(**kwargs):
    base_dir = kwargs['base_dir']
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%dT%H-%M')

    fname = os.path.join(base_dir, execution_date, 'objects.csv')
    with open(fname, 'r') as f:
        for line in f.readlines():
            print('==> %s' % line)

    return 0


base_dir = '/tmp/data/'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': utils.dates.days_ago(2),
    'provide_context': True
}

#pref_outs = File("/tmp/{{ execution_date }}.dat")

dag = DAG('jj-first', default_args=default_args, schedule_interval=timedelta(1))

get_prefixes_task = PythonOperator(task_id='get_prefixes',
                                   python_callable=get_objectnames,
                                   op_kwargs={'base_dir': base_dir}, dag=dag)

add_to_database_task = PythonOperator(task_id='add_to_db',
                                      python_callable=add_to_database,
                                      op_kwargs={'base_dir': base_dir},
                                      dag=dag)

get_prefixes_task.set_downstream(add_to_database_task)
