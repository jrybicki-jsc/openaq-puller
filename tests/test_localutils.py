from airflow_workflows.localutils import get_prefix_from_template, generate_fname, filter_file_list
import os
import unittest

from mys3utils.object_list import FileBasedObjectList
from datetime import date, datetime, timezone


class TestLocalUtils(unittest.TestCase):

    def setUp(self):
        pass

    def test_get_prefix_from_template(self):
        pr = get_prefix_from_template(
            execution_date=date(2016, 12, 24))
        self.assertEqual('test-realtime-gzip/2016-12-24/', pr)

    def test_generate_fname(self):
        ret = generate_fname(suffix='sfx', base_dir='/tmp/',
                             execution_date='2016-12-24')
        print(ret)
        self.assertEqual('/tmp/2016-12-24/sfx', ret)
        self.assertTrue(os.path.isdir('/tmp/2016-12-24/'))

        os.rmdir('/tmp/2016-12-24/')

    def test_filter_objects(self):
        #'Analyzing /tmp/target/realtime-gzipped/2019-01-17/1547689266.ndjson.gz'
        fl = ['/tmp/target/realtime-gzipped/2019-01-11/1547187666.ndjson.gz', '/tmp/target/realtime-gzipped/2019-01-11/1547195465.ndjson.gz',
              '/tmp/target/realtime-gzipped/2019-01-11/1547196665.ndjson.gz', '/tmp/target/realtime-gzipped/2019-01-11/1547200866.ndjson.gz', 
              '/tmp/target/realtime-gzipped/2019-01-11/1547170266.ndjson.gz', '/tmp/target/realtime-gzipped/2019-01-11/1547166066.ndjson.gz', 
              '/tmp/target/realtime-gzipped/2019-01-11/1547199066.ndjson.gz', '/tmp/target/realtime-gzipped/2019-01-11/1547173265.ndjson.gz', 
              '/tmp/target/realtime-gzipped/2019-01-11/1547176867.ndjson.gz', '/tmp/target/realtime-gzipped/2019-01-11/1547164867.ndjson.gz', 
              '/tmp/target/realtime-gzipped/2019-01-11/1547167864.ndjson.gz', '/tmp/target/realtime-gzipped/2019-01-11/1547193666.ndjson.gz', 
              '/tmp/target/realtime-gzipped/2019-01-11/1547190665.ndjson.gz', '/tmp/target/realtime-gzipped/2019-01-11/1547168466.ndjson.gz', 
              '/tmp/target/realtime-gzipped/2019-01-11/1547175667.ndjson.gz', '/tmp/target/realtime-gzipped/2019-01-11/1547176266.ndjson.gz', 
              '/tmp/target/realtime-gzipped/2019-01-11/1547179864.ndjson.gz', '/tmp/target/realtime-gzipped/2019-01-11/1547183467.ndjson.gz', 
              '/tmp/target/realtime-gzipped/2019-01-11/1547169664.ndjson.gz', '/tmp/target/realtime-gzipped/2019-01-11/1547180464.ndjson.gz', 
              '/tmp/target/realtime-gzipped/2019-01-11/1547178066.ndjson.gz', '/tmp/target/realtime-gzipped/2019-01-11/1547188267.ndjson.gz', 
              '/tmp/target/realtime-gzipped/2019-01-11/1547177466.ndjson.gz', '/tmp/target/realtime-gzipped/2019-01-11/1547199666.ndjson.gz', 
              '/tmp/target/realtime-gzipped/2019-01-11/1547200266.ndjson.gz', '/tmp/target/realtime-gzipped/2019-01-11/1547197266.ndjson.gz', 
              '/tmp/target/realtime-gzipped/2019-01-11/1547181066.ndjson.gz', '/tmp/target/realtime-gzipped/2019-01-11/1547182266.ndjson.gz', 
              '/tmp/target/realtime-gzipped/2019-01-11/1547190065.ndjson.gz', '/tmp/target/realtime-gzipped/2019-01-11/1547172067.ndjson.gz']
             
        filtered = filter_file_list(flist=fl, previous_run = datetime(2019,1,10), next_run=datetime(2019,1,12))
        self.assertEqual(len(filtered), len(fl))

        f2 = filter_file_list(flist=fl, previous_run = datetime(2019,1,10), next_run=datetime(2019,1,11, 9, 50))
        self.assertLess(len(f2), len(fl))
        self.assertEqual(24, len(f2))

        f3 = filter_file_list(flist=fl, previous_run = datetime(2019,1,9), next_run=datetime(2019,1,10, 9, 50))
        self.assertEqual(0, len(f3))


if __name__ == "__main__":
    unittest.main()
