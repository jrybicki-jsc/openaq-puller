import json

from models.Measurement import MeasurementDAO
from models.Series import SeriesDAO
from models.StationMeta import MyEngine, StationMetaCoreDAO
from mys3utils.tools import split_record
from nose.tools import raises
from sqlalchemy.exc import IntegrityError
from mys3utils.tools import get_jsons_from_stream

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


def __get__daos():
    engine = __get_engine()
    smdao = StationMetaCoreDAO(engine)
    smdao.create_table()

    dao = SeriesDAO(engine)
    dao.create_table()

    mdao = MeasurementDAO(engine)
    mdao.create_table()

    return smdao, dao, mdao


def __clean_daos(smdao, dao, mdao):
    mdao.drop_table()
    dao.drop_table()
    smdao.drop_table()

def test_multiple_inserts():
    smdao, dao, mdao = __get__daos()

    with open('./tests/series.ndjson', 'rb') as f:
        ll = list(get_jsons_from_stream(stream=f, object_name='series.ndjson'))
        assert len(ll) == 15

    for rec in ll:
        station, measurement, _ = split_record(rec)
        station_id = smdao.store_from_json(rec)
        series_id = dao.store(station_id=station_id, 
            parameter = measurement['parameter'],
            unit=measurement['unit'],
            averagingPeriod=f"{measurement['averagingPeriod']}")
        
        mes_id = mdao.store(series_id=series_id, value=measurement['value'], date=measurement['date']['utc'])
     
    stations = smdao.get_all()
    assert len(stations)==4

    series = dao.get_all()
    assert len(series) == 6

    mes = mdao.get_all()
    assert len(mes) == 15

    __clean_daos(smdao, dao, mdao)


def test_store_unique_series():
    smdao, dao, mdao = __get__daos()

    asj = json.loads(jseg)
    id1 = smdao.store_from_json(asj)
    assert id1 is not None

    station, measurement, _ = split_record(json.loads(jseg))

    res1 = dao.store(station_id=id1,
                    parameter=measurement['parameter'],
                    unit=measurement['unit'],
                    averagingPeriod=""
                    )

    
    res2 = dao.store(station_id=id1,
                    parameter=measurement['parameter'],
                    unit=measurement['unit'],
                    averagingPeriod=""
                    )


    print(f'{res1} == {res2}')
    assert res2 is not None
    assert res2 == res1

    result = dao.get_all()
    assert len(result) == 1
    dao.drop_table()


def test_get_forstation():
    smdao, dao, mdao = __get__daos()

    with open('./tests/series.ndjson', 'rb') as f:
        ll = list(get_jsons_from_stream(stream=f, object_name='series.ndjson'))
        assert len(ll) == 15

    for rec in ll:
        _, measurement, _ = split_record(rec)
        station_id = smdao.store_from_json(rec)
        series_id = dao.store(station_id=station_id, 
            parameter = measurement['parameter'],
            unit=measurement['unit'],
            averagingPeriod=f"{measurement['averagingPeriod']}")
        
        mes_id = mdao.store(series_id=series_id, value=measurement['value'], date=measurement['date']['utc'])
    mes = mdao.get_all()
    assert len(mes) == 15

    only_one = dao.get_all_for_station(station_id="Nisekh")
    assert len(only_one) == 1

    more_than_one = dao.get_all_for_station(station_id="Sankt Eriksgatan")
    assert len(more_than_one) == 3

    my_series = dao.get_for_id(series_id=1)
    assert my_series[2] == 'pm10'

    __clean_daos(smdao, dao, mdao)

