import unittest
from datetime import date, datetime, timezone
from unittest.mock import MagicMock
import logging

from mys3utils.DBFileList import DBBasedObjectList, get_engine, drop_all

mylist = [{'Key': 'realtime/2018-07-21/1532131426.ndjson',
           'LastModified': datetime(2018, 7, 21, 0, 3, 48, tzinfo=timezone.utc),
           'ETag': '"5c829c6252f6515487d9e853fe386875"',
           'Size': 4239686,
           'StorageClass': 'STANDARD'},
          {'Key': 'realtime/2018-07-21/1532132035.ndjson',
           'LastModified': datetime(2018, 7, 21, 0, 13, 57, tzinfo=timezone.utc),
           'ETag': '"ad382fc65deaef62c3a33e913bb4b768"',
           'Size': 5194596,
           'StorageClass': 'STANDARD'},
          {'Key': 'realtime/2018-07-21/1532132641.ndjson',
           'LastModified': datetime(2018, 7, 21, 0, 24, 2, tzinfo=timezone.utc),
           'ETag': '"e97ebf4c812ebbf97d9adeb150780383"',
           'Size': 4626081,
           'StorageClass': 'STANDARD'},
          {'Key': 'realtime/2018-07-21/1532133236.ndjson',
           'LastModified': datetime(2018, 7, 21, 0, 33, 58, tzinfo=timezone.utc),
           'ETag': '"dba3e62d278bdf26a816bcd60ac62b49"',
           'Size': 3759330,
           'StorageClass': 'STANDARD'},
          {'Key': 'realtime/2018-07-21/1532133849.ndjson',
           'LastModified': datetime(2018, 7, 21, 0, 44, 10, tzinfo=timezone.utc),
           'ETag': '"6703b0a76b375c0db0726f5bfaf15068"',
           'Size': 4599735,
           'StorageClass': 'STANDARD'}, ]


class TestDBList(unittest.TestCase):

    def tearDown(self):
        engine = get_engine()
        drop_all(engine)


    def test_load(self):
        kwargs = dict()
        kwargs['engine'] = get_engine()

        pfl = DBBasedObjectList(
            prefix='realtime/2018-07-21/', execution_date=datetime(2018, 7, 21), **kwargs)
        pfl._retrieve = MagicMock(return_value=mylist.copy())
        pfl.update()

        self.assertEqual(len(pfl.get_list()), 5)
        pfl._retrieve.assert_called_once()

        pfl = DBBasedObjectList(
            prefix='realtime/2018-07-21/', execution_date=datetime(2018, 7, 21), **kwargs)

        logging.info('This is the new test!')
        pfl._retrieve = MagicMock(return_value=[])
        pfl.load()
        pfl._retrieve.assert_not_called()
        self.assertEqual(5, len(pfl.get_list()))

    def test_load2(self):
        pfl = DBBasedObjectList(
            prefix='realtime/2018-07-21/', execution_date=datetime(2018, 7, 21), engine=get_engine())
        pfl._retrieve = MagicMock(return_value=mylist.copy())
        pfl.load()
        pfl.update()
        pfl._retrieve = MagicMock(return_value=mylist.copy())
        pfl.update()
        pfl._retrieve = MagicMock(return_value=mylist.copy())
        pfl.update()

        self.assertEqual(5, len(pfl.get_list()))
        