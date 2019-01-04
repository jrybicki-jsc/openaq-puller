import json
import os

from models.StationMeta import MyEngine, StationMetaCoreDAO
from mys3utils.tools import split_record
import unittest

import logging

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


class TestModels(unittest.TestCase):

    def setUp(self):
        # host empty is memory storage
        host = os.getenv('DB_HOST', '')
        dbname = os.getenv('DB_TEST_DATABASE', 'openaq')
        user = os.getenv('DB_USER', 'postgres')
        password = os.getenv('DB_PASS', 'mysecretpassword')
        self.engine = MyEngine(host=host, dbname=dbname, user=user, password=password)

        self.dao = StationMetaCoreDAO(self.engine)
        self.dao.drop_table()
        self.dao.create_table()

    def tearDown(self):
        self.dao.drop_table()


    def test_store_intodb(self):
        asj = json.loads(jseg)
        res = self.dao.store_from_json(asj)
        self.assertIsNotNone(res)
        self.assertEqual(res, "La Florida")

        result = self.dao.get_all()
        self.assertEqual(1, len(result))


    def test_store_unique(self):
        asj = json.loads(jseg)
        id1 = self.dao.store_from_json(asj)
        self.assertIsNotNone(id1)

        id2 = self.dao.store_from_json(asj)
        print('{} == {}'.format(id1, id2))
        self.assertIsNotNone(id2)
        logging.info('Getting all')
        result = self.dao.get_all()
   
        self.assertEqual(1, len(result))
        

    def test_split_record(self):
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


    def test_get_for_name(self):
        
        asj = json.loads(jseg)
        self.dao.store_from_json(asj)

        res = self.dao.get_for_name(station_name='foo')
        self.assertIsNone(res)

        res = self.dao.get_for_name(station_name="La Florida")
        self.assertIsNotNone(res)
        self.assertEqual(res[0], "La Florida")

    def test_limited(self):
        res = self.dao.get_limited(limit=20, offset=10)
        self.assertLess(len(res), 20)

        for i in range(100):
            self.dao.store(station_id=f'{i}station', station_name=f'name_{i}', station_location='Street', station_latitude=1.2, station_longitude=0.2, station_altitude=0.0,
              station_country='DE', station_state='NRW')

        res = self.dao.get_limited(limit=20, offset=10)
        self.assertEqual(len(res), 20)
        self.assertEqual('10station', res[0][0])
        
        res2 = self.dao.get_limited(limit=5, offset=20)
        self.assertEqual(len(res2), 5)
        self.assertEqual('20station', res2[0][0])


