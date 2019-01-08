import shutil
import unittest
from datetime import date, datetime, timezone
from unittest.mock import MagicMock
import os

from mys3utils.object_list import FileBasedObjectList

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


class TestObjList(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        shutil.rmtree('./tests/2018-07-21/', ignore_errors=True, onerror=None)


    def test_load(self):
        kwargs = dict()
        kwargs['base_dir'] = './tests/'
        pfl = FileBasedObjectList(
            prefix='test', execution_date=date(2018, 6, 14), **kwargs)
        pfl.load()
        self.assertEqual(91, len(pfl.get_list()))

    def test_fetch(self):
        pfl = FileBasedObjectList(
            prefix='realtime/2018-07-21/', execution_date=date(2018, 7, 21), base_dir='./tests/')
        pfl._retrieve = MagicMock(return_value=mylist)
        pfl.load()
        self.assertEqual(5, len(pfl.get_list()))

        pfl2 = FileBasedObjectList(
            prefix='realtime/2018-07-21/', execution_date=date(2018, 7, 21), base_dir='./tests/')
        pfl2.load()
        self.assertEqual(5, len(pfl.get_list()))
        self.assertEqual(5, len(pfl2.get_list()))

    def test_multiple_loads(self):
        pfl = FileBasedObjectList(prefix='realtime/2018-07-21/', execution_date=date(2018, 7, 21), base_dir='./tests/')
        pfl._retrieve = MagicMock(return_value=mylist)
        
        pfl.load()
        pfl._retrieve.assert_called_once()
        
        self.assertEqual(5, len(pfl.get_list()))

        pfl.load()
        self.assertEqual(5, len(pfl.get_list()))

    def test_multiple_stores(self):
        pfl = FileBasedObjectList(prefix='realtime/2018-07-21/', execution_date=date(2018, 7, 21), base_dir='./tests/')
        pfl._retrieve = MagicMock(return_value=mylist.copy())
        
        pfl.update()
        pfl._retrieve.assert_called_once()
        
        self.assertEqual(5, len(pfl.get_list()))

        pfl.update()
        pfl.update()
        pfl.update()
        pfl.update()
        


    def test_namings(self):
        pfl = FileBasedObjectList(prefix='realtime/2018-07-21/', execution_date=date(2018, 7, 21), base_dir='./tests/')
        pfl._retrieve = MagicMock(return_value=mylist)
        
        pfl.load()
        self.assertEqual(5, len(pfl.get_list()))
        pfl.load()

        for rec in pfl.get_list():
            self.assertIn('Name', rec)


        shutil.rmtree('./tests/2018-07-21/', ignore_errors=True, onerror=None)
        p2 = FileBasedObjectList(prefix='realtime/2018-07-21/', execution_date=date(2018, 7, 21), base_dir='./tests/')
        p2._retrieve = MagicMock(return_value=mylist)
        p2.load()
        self.assertEqual(5, len(p2.get_list()))
        for rec in p2.get_list():
            self.assertIn('Name', rec)

    def test_substract_file_list(self):
        pfl = FileBasedObjectList(prefix='realtime/2018-07-21/', execution_date=date(2018, 7, 21), base_dir='./tests/')
        pfl._retrieve = MagicMock(return_value=mylist)
        pfl.load()

        file_list = list()
        base_dir = '/mnt/volume/openaq-data/'
        for i in range(2):
            file_list.append(os.path.join(base_dir,mylist[i]['Key']))
     
        pfl.substract_list(file_list = file_list, base_dir=base_dir)
        self.assertEqual(3, len(pfl.get_list()))
        


