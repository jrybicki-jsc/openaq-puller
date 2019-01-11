import os
import logging

from mys3utils.tools import get_jsons_from_object, FETCHES_BUCKET, split_record
from mys3utils.DBFileList import DBBasedObjectList
from mys3utils.object_list import FileBasedObjectList
from airflow.hooks.postgres_hook import PostgresHook

from models.Measurement import MeasurementDAO
from models.Series import SeriesDAO
from models.StationMeta import StationMetaCoreDAO
from airflow.models import Variable
from string import Template
import types

import boto3
import botocore
import glob
import concurrent.futures


def get_prefix_from_template(**kwargs):
    date = kwargs['execution_date']
    try:
        prefix_pattern = Variable.get('prefix_pattern')
    except:
        logging.warning(
            'No prefix pattern provided (use prefix_pattern variable)')
        prefix_pattern = 'test-realtime-gzip/$date/'

    if prefix_pattern.startswith('/'):
        logging.warning('You probably dont want to start prefix with /')
    
    if '$date' not in prefix_pattern:
        logging.warning('No date placeholder available (use $date). Using pattern as prefix')
        return prefix_pattern
    
    return Template(prefix_pattern).substitute(date=date.strftime('%Y-%m-%d')).strip()

def generate_fname(suffix, base_dir, execution_date):
    fname = os.path.join(base_dir, execution_date)

    os.makedirs(fname, exist_ok=True)
    fname = os.path.join(fname, suffix)
    return fname


def add_to_db(station_dao, series_dao, mes_dao, station, measurement):
    try:
        stat_id = station_dao.store_from_json(station)
        series_id = series_dao.store(station_id=stat_id,
                  parameter=measurement['parameter'],
                  unit=measurement['unit'],
                  averagingPeriod=f"{ measurement['averagingPeriod']} ")

        mes_dao.store(series_id=series_id, value=measurement['value'], date=measurement['date']['utc'])
    except:
        logging.warning(f'Problem storing { station } { measurement }. Skipped')


def local_process_file(object_name):
    ret = []
    for record in get_jsons_from_object(bucket=FETCHES_BUCKET, object_name=object_name):
        station, measurement, _ = split_record(record)

        ret.append([station, measurement])
    return ret


def get_file_list(prefix, **kwargs):
    try:
        pg = PostgresHook(postgres_conn_id='file_list_db')
        engine = pg.get_sqlalchemy_engine()
        logging.info('Using db-based object list')
        fl = DBBasedObjectList(engine=engine, prefix=prefix, **kwargs)
    except:
        logging.info(f'Using file-based object list for { prefix }')
        fl = FileBasedObjectList(prefix=prefix, **kwargs)

    return fl

def setup_daos():
    try:
        pg = PostgresHook(postgres_conn_id='openaq-db')    
    except:
        logging.error('Remote database not defined. Use [openaq-db] connection')
        return None

    wrapper = types.SimpleNamespace()
    wrapper.get_engine = pg.get_sqlalchemy_engine
    station_dao = StationMetaCoreDAO(engine=wrapper)
    series_dao = SeriesDAO(engine=wrapper)
    mes_dao = MeasurementDAO(engine=wrapper)

    station_dao.create_table()
    series_dao.create_table()
    mes_dao.create_table()

    return station_dao, series_dao, mes_dao

def print_db_stats(station_dao, series_dao, mes_dao):
    logging.info(f"{ station_dao.count()} stations stored in db")
    logging.info(f"{ series_dao.count()} series stored in db")
    logging.info(f"{ mes_dao.count()} measurements stored in db")


def generate_object_list(*args, **kwargs):
    prefix = get_prefix_from_template(**kwargs)
    logging.info(f'Will be getting objects for {prefix}')
    pfl = get_file_list(prefix=prefix, **kwargs)
    pfl.update()

    return True

def list_directory(target_dir):
    mdir=os.path.join(target_dir, '*')
    return [f for f_ in [glob.glob(f'{mdir}.{ext}') for ext in ('ndjson', 'ndjson.gz')] for f in f_]


def download_and_store(**kwargs):
    prefix = get_prefix_from_template(**kwargs)
    
    target_dir = os.path.join(Variable.get('target_dir'), prefix)
    os.makedirs(target_dir, exist_ok=True)
    flist = glob.glob(os.path.join(target_dir, '*'))
    logging.info(f'Files detected in {target_dir}: { len(flist)}')

    pfl = get_file_list(prefix=prefix, **kwargs)
    pfl.load()
    tdir = Variable.get('target_dir')
    pfl.substract_list(file_list=flist, base_dir=tdir)

    logging.info(f'Downloading {len(pfl.get_list())} objects from {prefix} to {target_dir}')

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

    return True
    