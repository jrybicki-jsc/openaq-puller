from airflow_workflows.localutils import get_prefix_from_template
import datetime

import unittest

class TestLocalUtils(unittest.TestCase):

    def setUp(self):
        pass

    def test_get_prefix_from_template(self):
        pr = get_prefix_from_template(execution_date=datetime.date(2016,12, 24))
        self.assertEqual('test-realtime-gzip/2016-12-24/', pr)


if __name__ == "__main__":
    unittest.main()