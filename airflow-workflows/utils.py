import os

from mys3utils.tools import get_jsons_from_object, FETCHES_BUCKET, split_record


def generate_fname(suffix, **kwargs):
    base_dir = kwargs['base_dir']
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%dT%H-%M')

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
        station, measurement, ext = split_record(record)

        ret.append([station, measurement])

    return ret

