import os
import logging

from mys3utils.tools import get_jsons_from_object, FETCHES_BUCKET, split_record
from mys3utils.DBFileList import DBBasedObjectList
from mys3utils.object_list import FileBasedObjectList
from airflow.hooks.postgres_hook import PostgresHook


def generate_fname(suffix, base_dir, execution_date):

    fname = os.path.join(base_dir, execution_date)

    os.makedirs(fname, exist_ok=True)
    fname = os.path.join(fname, suffix)
    return fname


def add_to_db(station_dao, mes_dao, station, measurement):
    stat_id = station_dao.store_from_json(station)
    mes_dao.store(station_id=stat_id,
                  parameter=measurement['parameter'],
                  value=measurement['value'],
                  unit=measurement['unit'],
                  averagingPeriod=measurement['averagingPeriod'],
                  date=measurement['date']['utc'])


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