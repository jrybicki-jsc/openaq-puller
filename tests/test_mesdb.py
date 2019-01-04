import json

from models.Measurement import MeasurementDAO
from models.Series import SeriesDAO
from models.StationMeta import MyEngine, StationMetaCoreDAO
from mys3utils.tools import split_record
from nose.tools import raises
from sqlalchemy.exc import IntegrityError
from mys3utils.tools import get_jsons_from_stream

import os
import unittest

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


class TestMessDB(unittest.TestCase):

    def setUp(self):
        # host empty is memory storage
        host = os.getenv('DB_HOST', '')
        dbname = os.getenv('DB_DATABASE', 'openaq')
        user = os.getenv('DB_USER', 'jj')
        password = os.getenv('DB_PASS', 's3cret')
        self.engine = MyEngine(host=host, dbname=dbname, user=user, password=password)

        self.smdao = StationMetaCoreDAO(self.engine)
        self.smdao.create_table()

        self.dao = SeriesDAO(self.engine)
        self.dao.create_table()

        self.mdao = MeasurementDAO(self.engine)
        self.mdao.create_table()

    def tearDown(self):
        self.dao.drop_table()
        self.mdao.drop_table()
        self.smdao.drop_table()


    def test_store_intodb(self):
        
        station, measurement, _ = split_record(json.loads(jseg))

        station_id = self.smdao.store_from_json(station)
        res = self.smdao.get_all()
        self.assertEqual(len(res), 1)

        series_id = self.dao.store(station_id=station_id,
                        parameter=measurement['parameter'],
                        unit=measurement['unit'],
                        averagingPeriod="")
        result = self.dao.get_all()
        self.assertEqual(len(result), 1)

        mes_id = self.mdao.store(series_id=series_id, value=measurement['value'], date=measurement['date']['utc'])
        self.assertTrue(mes_id)
        result = self.mdao.get_all()
        self.assertEqual(len(result), 1)


    def test_insert_all(self):

        station, measurement, _ = split_record(json.loads(jseg))
        station_id = self.smdao.store_from_json(station)

        series_id = self.dao.store(station_id=station_id,
                        parameter=measurement['parameter'],
                        unit=measurement['unit'],
                        averagingPeriod=""
                        )

        result = self.dao.get_all()
        self.assertEqual(len(result), 1)

        mes_id = self.mdao.store(series_id=series_id, value=measurement['value'], date=measurement['date']['utc'])
        self.assertTrue(mes_id)
        res = self.mdao.get_all()
        self.assertEqual(len(res), 1)


    @raises(IntegrityError)
    def test_foreignkey_violation(self):

        station, measurement, _ = split_record(json.loads(jseg))

        res = self.dao.store(station_id='non-existing name',
                        parameter=measurement['parameter'],
                        unit=measurement['unit'],
                        averagingPeriod=""
                        )
        result = self.dao.get_all()
        assert len(result) == 1
        
        if self.engine.get_engine().name == 'sqlite':
            raise IntegrityError(statement='SQLLITE', params='does not support', orig='remote key violation',
                                connection_invalidated=False, code=None)

    def test_multiple_inserts(self):

        with open('./tests/series.ndjson', 'rb') as f:
            ll = list(get_jsons_from_stream(stream=f, object_name='series.ndjson'))
            self.assertEqual(len(ll), 15)

        for rec in ll:
            station, measurement, _ = split_record(rec)
            station_id = self.smdao.store_from_json(rec)
            series_id = self.dao.store(station_id=station_id, 
                parameter = measurement['parameter'],
                unit=measurement['unit'],
                averagingPeriod=f"{measurement['averagingPeriod']}")
            
            mes_id = self.mdao.store(series_id=series_id, value=measurement['value'], date=measurement['date']['utc'])
        
        stations = self.smdao.get_all()
        self.assertEqual(len(stations),4)

        series = self.dao.get_all()
        self.assertEqual(len(series),6)

        mes = self.mdao.get_all()
        self.assertEqual(len(mes), 15)


    def test_store_unique_series(self):

        asj = json.loads(jseg)
        id1 = self.smdao.store_from_json(asj)
        self.assertIsNotNone(id1) 

        station, measurement, _ = split_record(json.loads(jseg))

        res1 = self.dao.store(station_id=id1,
                        parameter=measurement['parameter'],
                        unit=measurement['unit'],
                        averagingPeriod=""
                        )

        
        res2 = self.dao.store(station_id=id1,
                        parameter=measurement['parameter'],
                        unit=measurement['unit'],
                        averagingPeriod=""
                        )


        print(f'{res1} == {res2}')
        self.assertIsNotNone(res2)
        self.assertIsNotNone(res1)
        self.assertEqual(res1, res2)

        result = self.dao.get_all()
        self.assertEqual(1, len(result))


    def test_get_forstation(self):
        with open('./tests/series.ndjson', 'rb') as f:
            ll = list(get_jsons_from_stream(stream=f, object_name='series.ndjson'))
            self.assertEqual(15, len(ll))

        for rec in ll:
            _, measurement, _ = split_record(rec)
            station_id = self.smdao.store_from_json(rec)
            series_id = self.dao.store(station_id=station_id, 
                parameter = measurement['parameter'],
                unit=measurement['unit'],
                averagingPeriod=f"{measurement['averagingPeriod']}")
            
            mes_id = self.mdao.store(series_id=series_id, value=measurement['value'], date=measurement['date']['utc'])
        mes = self.mdao.get_all()
        self.assertEqual(15, len(mes))

        only_one = self.dao.get_all_for_station(station_id="Nisekh")

        self.assertEqual(1, len(only_one))
        
        more_than_one = self.dao.get_all_for_station(station_id="Sankt Eriksgatan")
        
        self.assertEqual(3, len(more_than_one))
        my_series = self.dao.get_for_id(series_id=1)

        self.assertEqual(my_series[2],'pm10')

    def test_oversized(self):
        st = {'location': 'SLAVONSKI BROD-1 - RH0109 - HR0010A; Državna mreža za trajno praćenje kvalitete zraka praćenje kvalitete zrakapraćenje kvalitete zrakapraćenje kvalitete zraka', 
        'city': 'Državna mreža za trajno praćenje kvalitete zraka - RH01', 
        'station_name': 'SLAVONSKI BROD-1 - RH0109 - HR0010A; Državna mreža za trajno praćenje kvalitete zraka', 
        'coordinates': {'latitude': 45.15947222168539, 
        'longitude': 17.995100000000004}, 
        'station_altitude': 0, 'country': 'HR', 'station_state': ''}
        idd = self.smdao.store_from_json(st)
        print(f' {idd}')
        self.assertEqual(len(idd), 64)

 
    def test_oversized2(self):
        idd = self.smdao.store(station_id='A'*513, station_name='N'*512, station_country='HR', station_location='loc', station_latitude=0.0, station_longitude=0.0, station_altitude=0.0, station_state='Rijeka')
        self.assertEqual(len(idd), 64)

    def test_counter(self):
        with open('./tests/series.ndjson', 'rb') as f:
            ll = list(get_jsons_from_stream(stream=f, object_name='series.ndjson'))
            self.assertEqual(15, len(ll))

        for rec in ll:
            _, measurement, _ = split_record(rec)
            station_id = self.smdao.store_from_json(rec)
            series_id = self.dao.store(station_id=station_id, 
                parameter = measurement['parameter'],
                unit=measurement['unit'],
                averagingPeriod=f"{measurement['averagingPeriod']}")
            
            mes_id = self.mdao.store(series_id=series_id, value=measurement['value'], date=measurement['date']['utc'])
        mes = self.mdao.get_all()
        self.assertEqual(15, len(mes))
        self.assertEqual(self.mdao.count(), 15)

        self.assertEqual(4, self.smdao.count())
        self.assertEqual(6, self.dao.count())

        self.assertEqual(4, self.mdao.count(series_id=1))
        self.assertEqual(0, self.mdao.count(series_id=838232))

        self.assertEqual(0, self.dao.count(station_id=1212))


    



