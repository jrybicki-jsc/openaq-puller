from airflow_workflows.localutils import get_prefix_from_template, generate_fname
import datetime
import os
import unittest

class TestLocalUtils(unittest.TestCase):

    def setUp(self):
        pass

    def test_get_prefix_from_template(self):
        pr = get_prefix_from_template(execution_date=datetime.date(2016,12, 24))
        self.assertEqual('test-realtime-gzip/2016-12-24/', pr)


    def test_generate_fname(self):
        ret = generate_fname(suffix='sfx', base_dir='/tmp/', execution_date='2016-12-24')
        print(ret)
        self.assertEqual('/tmp/2016-12-24/sfx', ret)
        self.assertTrue(os.path.isdir('/tmp/2016-12-24/'))

        os.rmdir('/tmp/2016-12-24/')
        



if __name__ == "__main__":
    unittest.main()