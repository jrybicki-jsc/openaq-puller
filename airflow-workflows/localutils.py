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


def get_prefix_from_template(**kwargs):
    date = kwargs['execution_date']
    prefix_pattern = Variable.get('prefix_pattern')
    if prefix_pattern is None:
        logging.warning(
            'No prefix pattern provided (use prefix_pattern variable)')
        prefix_pattern = 'test-realtime-gzip/$date/'
    return Template(prefix_pattern).substitute(date=date.strftime('%Y-%m-%d')).strip()

def generate_fname(suffix, base_dir, execution_date):
    fname = os.path.join(base_dir, execution_date)

    os.makedirs(fname, exist_ok=True)
    fname = os.path.join(fname, suffix)
    return fname


def add_to_db(station_dao, series_dao, mes_dao, station, measurement):
    stat_id = station_dao.store_from_json(station)
    series_id = series_dao.store(station_id=stat_id,
                  parameter=measurement['parameter'],
                  unit=measurement['unit'],
                  averagingPeriod=f"measurement['averagingPeriod']")

    mes_dao.store(series_id=series_id, value=measurement['value'], date=measurement['date']['utc'])



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
        logging.info(f'Using file-based object list for { prefix} ')
        fl = FileBasedObjectList(prefix=prefix, **kwargs)

    return fl

def setup_daos():
    pg = PostgresHook(postgres_conn_id='openaq-db')
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
    logging.info(f"{ len(station_dao.get_all())} stations stored in db")
    logging.info(f"{ len(series_dao.get_all())} series stored in db")
    logging.info(f"{ len(mes_dao.get_all())} measurements stored in db")
    