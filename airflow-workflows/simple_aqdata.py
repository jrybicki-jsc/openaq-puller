import concurrent.futures
import logging
import types
from datetime import datetime, timedelta
from string import Template

import boto3
import botocore
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from models.Measurement import MeasurementDAO
from models.StationMeta import StationMetaCoreDAO
from mys3utils.tools import (FETCHES_BUCKET, get_jsons_from_object,
                             get_object_list, split_record)
from sqlalchemy import (Column, DateTime, ForeignKey, Integer, MetaData,
                        Sequence, String, Table, create_engine, select)
from sqlalchemy.sql import and_

from localutils import get_file_list

metadata = MetaData()

objects_meta = Table('object', metadata,
                     Column('prefix_check', None,
                            ForeignKey('prefix_checks.id')),
                     Column('name', String(128)),
                     Column('size', Integer),
                     Column('checksum', String(34)),
                     Column('created', DateTime),
                     keep_existing=True,
                     )


def add_to_db(station_dao, mes_dao, station, measurement):
    stat_id = station_dao.store_from_json(station)
    mes_dao.store(station_id=stat_id,
                  parameter=measurement['parameter'],
                  value=measurement['value'],
                  unit=measurement['unit'],
                  averagingPeriod=measurement['averagingPeriod'],
                  date=measurement['date']['utc'])


def setup_daos():
    pg = PostgresHook(postgres_conn_id='openaq-db')
    wrapper = types.SimpleNamespace()
    wrapper.get_engine = pg.get_sqlalchemy_engine
    station_dao = StationMetaCoreDAO(engine=wrapper)
    mes_dao = MeasurementDAO(engine=wrapper)
    station_dao.create_table()

    return mes_dao, station_dao


def generate_object_list(**kwargs):
    date = kwargs['execution_date']
    prefix = Template(kwargs['prefix-pattern']
                      ).substitute(date=date.strftime('%Y-%m-%d'))
    logging.info(f'Retrieving object names for prefix: { prefix }')

    pg = PostgresHook(postgres_conn_id='file_list_db')
    engine = pg.get_sqlalchemy_engine()

    client = boto3.client('s3', config=botocore.client.Config(
        signature_version=botocore.UNSIGNED))
    objects = client.list_objects_v2(
        Bucket=FETCHES_BUCKET, Prefix=prefix, Delimiter='/')

    for o in objects:
        ins = objects_meta.insert().values(
            prefix_check=prefix,
            name=o['Key'],
            size=o['Size'],
            checksum=o['ETag'],
            created=o['LastModified'])

        engine.execute(ins)


def transform_objects(**kwargs):
    date = kwargs['execution_date']
    prefix = Template(kwargs['prefix-pattern']
                      ).substitute(date=date.strftime('%Y-%m-%d'))

    pg = PostgresHook(postgres_conn_id='file_list_db')
    engine = pg.get_sqlalchemy_engine()

    mes_dao, station_dao = setup_daos()
    s = select([objects_meta]).where(objects_meta.c.prefix_check == prefix)
    all_objects = engine.execute(s).fetchall()
    for r in all_objects:
        object_name = r[1]
        for record in get_jsons_from_object(bucket=FETCHES_BUCKET, object_name=object_name):
            station, measurement, _ = split_record(record)

            add_to_db(station_dao, mes_dao, station=station,
                      measurement=measurement)

    logging.info(f'{ len(station_dao.get_all())} stations stored in db')
    logging.info(f'{ len (mes_dao.get_all())} measurements stored in db')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2013, 11, 23),
  #  'end_date': datetime(2018, 7, 31),
    'provide_context': True,
    'catchup': True
}

op_kwargs = {
    'prefix-pattern': 'realtime/$date/'
}

dag = DAG('simple-aqdata', default_args=default_args,
          schedule_interval=timedelta(1))

with dag:
    get_objects_task = PythonOperator(task_id='get_object_list',
                                      python_callable=generate_object_list,
                                      op_kwargs=op_kwargs)

    transform_and_store_task = PythonOperator(task_id='transform',
                                              python_callable=transform_objects,
                                              op_kwargs=op_kwargs)

get_objects_task >> transform_and_store_task
