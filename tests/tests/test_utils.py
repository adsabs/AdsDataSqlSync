import os, sys
import unittest

PROJECT_HOME = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(PROJECT_HOME)

from adsmsg.metrics_record import MetricsRecord as ProtobufMetricsRecord
from adsmsg.metrics_record import MetricsRecordList as ProtobufMetricsRecordList
from adsdata.utils import create_clean

class TestUtils(unittest.TestCase):
    
    def test_create_clean(self):

        # test metrics data
        m = {'refereed': True, 
             'bibcode': '1997BoLMe..85..475M', 
             'id': 3}
        
        x = create_clean(m)
        y = m.copy()
        del y['id']
        self.assertEqual(x, y)
        
if __name__ == '__main__':
    unittest.main(verbosity=2)

