import random
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, Mock

from mys3utils.tools import (filter_objects, filter_prefixes,
                             get_jsons_from_object, get_jsons_from_stream,
                             get_object_list, read_object_list, split_record)


class TestS3Tools(unittest.TestCase):

    def get_fake_list(self, start_date, count):
        day = timedelta(days=1)
        cdate = start_date
        lst = []
        for _ in range(0, count):
            lst.append({'Key': 'realtime/2013-11-27/2013-11-27.ndjson',
                        'LastModified': cdate,
                        'ETag': '"0214c4d59b099b041aa651c8221aa4f3"',
                        'Size': random.randint(100, 12001),
                        'StorageClass': 'STANDARD'})
            cdate += day

        return lst

    def test_filter_objects(self):
        start_date = datetime(2018, 1, 20, tzinfo=timezone.utc)
        lst = self.get_fake_list(start_date, 20)

        end_date = start_date + timedelta(days=5)
        start_date += timedelta(days=2)
        filtered = filter_objects(lst, start_date, end_date)
        filtered = list(filtered)

        self.assertLess(len(filtered), len(lst))
        self.assertNotIn(lst[0], filtered)
        self.assertNotIn(lst[-1], filtered)

    def test_filter_2(self):
        ll = [{'Key': 'test-realtime/2017-09-28/1506558318.ndjson',
               'LastModified': datetime(2017, 9, 28, 18, 25, 25, tzinfo=timezone.utc),
               'ETag': '"e8cbd704b68b3ebb16989e6c044c5b33"', 'Size': 4218442, 'StorageClass': 'STANDARD'},
              {'Key': 'test-realtime/2017-09-28/1506558901.ndjson',
               'LastModified': datetime(2017, 9, 28, 18, 25, 25, tzinfo=timezone.utc),
               'ETag': '"1f35bc3d6a428d03dd5135caca4fc3cf"', 'Size': 3433602, 'StorageClass': 'STANDARD'},
              {'Key': 'test-realtime/2017-09-28/1506560107.ndjson',
               'LastModified': datetime(2017, 9, 28, 18, 25, 25, tzinfo=timezone.utc),
               'ETag': '"c05f5be44d73128a5757fafad9009190"', 'Size': 1945276, 'StorageClass': 'STANDARD'},
              {'Key': 'test-realtime/2017-09-28/1506560722.ndjson',
               'LastModified': datetime(2017, 9, 28, 18, 25, 25, tzinfo=timezone.utc),
               'ETag': '"a2ec58d1c47ffe16a9b5e004d02048a1"', 'Size': 2355868, 'StorageClass': 'STANDARD'},
              {'Key': 'test-realtime/2017-09-28/1506561320.ndjson',
               'LastModified': datetime(2017, 9, 28, 18, 25, 25, tzinfo=timezone.utc),
               'ETag': '"2d3dfc237a1274e83e21d326f0b6d59a"', 'Size': 3129923, 'StorageClass': 'STANDARD'}
              ]
        filtered = filter_objects(all_objects=ll, start_date=datetime(2016, 8, 7, tzinfo=timezone.utc),
                                  end_date=datetime(2017, 9, 29))
        print(list(filtered))

    def test_filter_prefixes(self):
        ll = ['realtime/2014-03-02/',
              'realtime/2014-03-03/',
              'realtime/2014-03-04/',
              'realtime/2014-03-05/',
              'realtime/2014-03-06/',
              'realtime/2014-03-07/',
              'realtime/2014-03-08/',
              'realtime/2014-03-09/',
              'realtime/2014-03-10/',
              'realtime/2014-03-11/',
              'realtime/2014-03-12/']

        filtered = filter_prefixes(prefixes=ll, start_date=datetime(
            2014, 3, 5), end_date=datetime(2014, 3, 8))
        self.assertEqual(len(list(filtered)), 4)

    def test_filter_prefixes_nostart(self):
        ll = ['realtime/2014-03-02/',
              'realtime/2014-03-03/',
              'realtime/2014-03-04/',
              'realtime/2014-03-05/',
              'realtime/2014-03-06/',
              'realtime/2014-03-07/',
              'realtime/2014-03-08/',
              'realtime/2014-03-09/',
              'realtime/2014-03-10/',
              'realtime/2014-03-11/',
              'realtime/2014-03-12/']

        filtered = filter_prefixes(
            prefixes=ll, start_date=datetime.min, end_date=datetime(2014, 3, 8))
        self.assertEqual(7, len(list(filtered)))

    def test_filter_prefixes_manual(self):
        ll = ['realtime/2014-03-02/',
              'realtime/2014-03-03/',
              'realtime/2014-03-04/',
              'realtime/2014-03-05/',
              'realtime/2014-03-06/',
              'realtime/2014-03-07/',
              'realtime/2014-03-08/',
              'realtime/2014-03-09/',
              'realtime/2014-03-10/',
              'realtime/2014-03-11/',
              'realtime/2014-03-12/',
              'realtime/manual/']

        filtered = filter_prefixes(
            prefixes=ll, start_date=datetime.min, end_date=datetime(2014, 3, 8))
        self.assertEqual(7, len(list(filtered)))

    def test_get_objects(self):
        client = Mock()
        client.list_objects_v2 = MagicMock(return_value={
            'IsTruncated': False,
            'CommonPrefixes': [{'Prefix': 'realtime/2018-06-09/'},
                               {'Prefix': 'realtime/2018-06-10/'},
                               {'Prefix': 'realtime/2018-06-11/'},
                               {'Prefix': 'realtime/2018-06-12/'},
                               {'Prefix': 'realtime/2018-06-13/'},
                               {'Prefix': 'realtime/manual/'}],
            'Contents': ['/obj1', '/obj2', '/obj3']
        })
        obj, pref = get_object_list(
            bucket_name='bucket', prefix='/', client=client)
        client.list_objects_v2.assert_called_once()

        self.assertEqual(len(pref), 6)
        self.assertEqual(len(obj), 3)

    def test_read_object_list(self):
        with open('tests/test-objects.csv') as f:
            wl = read_object_list(f)

        self.assertEqual(40, len(wl))

        filtered = list(filter_objects(all_objects=wl, start_date=datetime(2017, 9, 27, tzinfo=timezone.utc),
                                       end_date=datetime(2017, 9, 29)))

        self.assertEqual(31,  len(filtered))

    def test_get_jsons_from_object(self):
        client = Mock()
        f = open('./tests/exobj.ndjson', 'rb')
        client.get_object = MagicMock(return_value={'Body': f})

        ll = list(get_jsons_from_object(
            bucket='bucket', object_name='obj', client=client))

        f.close()
        client.get_object.assert_called_once()

        self.assertEqual(7444, len(ll))

    def test_get_jsons_from_stream(self):
        with open('./tests/exobj.ndjson', 'rb') as f:
            ll = list(get_jsons_from_stream(
                stream=f, object_name='./tests/exobj.ndjson'))

        self.assertEqual(7444, len(ll))

    def test_get_jsons_from_gzipped_stream(self):
        with open('./tests/2014-03-30.ndjson.gz', 'rb') as f:
            ll = list(get_jsons_from_stream(
                stream=f, object_name='./tests/2014-03-30.ndjson.gz'))

        self.assertEqual(64, len(ll))

    def test_get_json_from_gzipped_object(self):
        client = Mock()
        f = open('./tests/2014-03-30.ndjson.gz', 'rb')
        client.get_object = MagicMock(return_value={'Body': f})

        ll = list(get_jsons_from_object(bucket='bucket',
                                        object_name='2014-03-30.ndjson.gz', client=client))

        f.close()
        client.get_object.assert_called_once()

        self.assertEqual(64, len(ll))

    def test_massive_split(self):
        client = Mock()
        f = open('./tests/exobj.ndjson', 'rb')
        client.get_object = MagicMock(return_value={'Body': f})

        for jl in get_jsons_from_object(bucket='bucket', object_name='obj', client=client):
            station, measurement, ext = split_record(jl)
            assert 'location' in station
            assert 'value' in measurement
            assert 'location' not in measurement
            assert 'city' in station
            assert 'city' not in ext
            assert 'location' not in ext
            assert 'attribution' in ext
            assert 'sourceName' in station
            assert 'sourceName' in measurement

        f.close()
