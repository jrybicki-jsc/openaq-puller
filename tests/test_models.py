import json

import os

from models.StationMeta import StationMetaCoreDAO, MyEngine
from mys3utils.tools import split_record

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
    dbname = os.getenv('DB_TEST_DATABASE', 'openaq')
    user = os.getenv('DB_USER', 'jj')
    password = os.getenv('DB_PASS', 's3cret')
    engine = MyEngine(host=host, dbname=dbname, user=user, password=password)

    return engine


def test_store_intodb():
    # host empty is memory storage
    engine = __get_engine()
    dao = StationMetaCoreDAO(engine)
    dao.drop_table()
    dao.create_table()

    asj = json.loads(jseg)
    res = dao.store_from_json(asj)
    assert res is not None
    assert res == "Chile - SINCA"

    result = dao.get_all()
    assert len(result) == 1
    dao.drop_table()


def test_store_unique():
    engine = __get_engine()
    dao = StationMetaCoreDAO(engine)
    dao.create_table()

    asj = json.loads(jseg)
    dao.store_from_json(asj)
    dao.store_from_json(asj)

    result = dao.get_all()
    assert len(result) == 1
    dao.drop_table()


def test_split_record():
    rec = """{"date":{"utc":"2018-06-06T23:00:00.000Z","local":"2018-06-07T05:00:00+06:00"},
           "parameter":"pm25",
           "location":"US Diplomatic Post: Dhaka",
           "value":27,
           "unit":"µg/m³",
           "city":"Dhaka",
           "attribution":[{"name":"EPA AirNow DOS","url":"http://airnow.gov/index.cfm?action=airnow.global_summary"}],
           "averagingPeriod":{"value":1,"unit":"hours"},
           "coordinates":{"latitude":23.796373,"longitude":90.424614},
           "country":"BD",
           "sourceName":"StateAir_Dhaka",
           "sourceType":"government",
           "mobile":false}"""
    jl = json.loads(rec)

    station, measurement, ext = split_record(jl)
    assert 'location' in station
    assert 'value' in measurement
    assert 'location' not in measurement
    assert 'city' in station
    assert 'city' not in ext
    assert 'location' not in ext
    assert 'attribution' in ext


def test_get_for_name():
    engine = __get_engine()
    dao = StationMetaCoreDAO(engine)
    dao.create_table()

    asj = json.loads(jseg)
    dao.store_from_json(asj)

    res = dao.get_for_name(station_name='foo')
    assert res is None

    res = dao.get_for_name(station_name="Chile - SINCA")
    assert res is not None
    assert res[0] == "Chile - SINCA"
    dao.drop_table()


