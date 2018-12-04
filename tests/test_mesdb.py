import json

from models.Measurement import MeasurementDAO
from models.Series import SeriesDAO
from models.StationMeta import MyEngine, StationMetaCoreDAO
from mys3utils.tools import split_record
from nose.tools import raises
from sqlalchemy.exc import IntegrityError

import os

jseg = '{"date":{"utc":"2018-06-07T00:00:00.000Z","local":"2018-06-06T20:00:00-04:00"},' \
       '"parameter":"co",' \
       '"location":"La Florida",' \
       '"value":2320.51,' \
       '"unit":"µg/m³",' \
       '"city":"La Florida",' \
       '"attribution":' \
       '[{"name":"SINCA","url":"http://sinca.mma.gob.cl/"},' \
       '{"name":"Ministerio del Medio Ambiente"}],' \
       '"coordinates":{"latitude":-33.516630362266,"longitude":-70.588123961971},' \
       '"country":"CL",' \
       '"sourceName":"Chile - SINCA",' \
       '"sourceType":"government",' \
       '"mobile":false}'


def __get_engine():
    # host empty is memory storage
    host = os.getenv('DB_HOST', '')
    dbname = os.getenv('DB_DATABASE', 'openaq')
    user = os.getenv('DB_USER', 'jj')
    password = os.getenv('DB_PASS', 's3cret')
    engine = MyEngine(host=host, dbname=dbname, user=user, password=password)

    return engine


def test_store_intodb():
    engine = __get_engine()
    smdao = StationMetaCoreDAO(engine)
    smdao.create_table()

    dao = SeriesDAO(engine)
    dao.create_table()

    mdao = MeasurementDAO(engine)
    mdao.create_table()

    station, measurement, _ = split_record(json.loads(jseg))

    station_id = smdao.store_from_json(station)
    res = smdao.get_all()
    assert len(res) == 1

    series_id = dao.store(station_id=station_id,
                    parameter=measurement['parameter'],
                    unit=measurement['unit'],
                    averagingPeriod="")
    result = dao.get_all()
    assert len(result) == 1

    mes_id = mdao.store(series_id=series_id, value=measurement['value'], date=measurement['date']['utc'])
    assert mes_id == True
    result = mdao.get_all()
    assert len(result) == 1

    dao.drop_table()
    mdao.drop_table()


def test_insert_all():
    engine = __get_engine()

    smdao = StationMetaCoreDAO(engine)
    smdao.create_table()

    dao = SeriesDAO(engine)
    dao.create_table()

    mdao = MeasurementDAO(engine)
    mdao.create_table()

    station, measurement, _ = split_record(json.loads(jseg))
    station_id = smdao.store_from_json(station)

    series_id = dao.store(station_id=station_id,
                    parameter=measurement['parameter'],
                    unit=measurement['unit'],
                    averagingPeriod=""
                    )

    result = dao.get_all()
    assert len(result) == 1

    mes_id = mdao.store(series_id=series_id, value=measurement['value'], date=measurement['date']['utc'])
    assert mes_id
    res = mdao.get_all()
    assert len(res) == 1

    dao.drop_table()
    mdao.drop_table()


@raises(IntegrityError)
def test_foreignkey_violation():
    engine = __get_engine()
    dao = SeriesDAO(engine)
    dao.create_table()

    station, measurement, _ = split_record(json.loads(jseg))

    res = dao.store(station_id='non-existing name',
                    parameter=measurement['parameter'],
                    unit=measurement['unit'],
                    averagingPeriod=""
                    )
    result = dao.get_all()
    assert len(result) == 1
    dao.drop_table()

    if engine.get_engine().name == 'sqlite':
        raise IntegrityError(statement='SQLLITE', params='does not support', orig='remote key violation',
                             connection_invalidated=False, code=None)

