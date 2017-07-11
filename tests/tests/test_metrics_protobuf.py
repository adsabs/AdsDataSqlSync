import os, sys
PROJECT_HOME = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(PROJECT_HOME)
import unittest

from adsmsg.metrics_record import MetricsRecord as ProtobufMetricsRecord
from adsmsg.metrics_record import MetricsRecordList as ProtobufMetricsRecordList
from adsdata.utils import create_clean

class test_serialize(unittest.TestCase):
    
    def test_serialize_database_record(self):
        """simple test to create protobuf from database like dict"""

        # test metrics data
        m = {'refereed': True, 
             'bibcode': '1997BoLMe..85..475M', 
             'rn_citation_data': [{'ref_norm': 0.2, 'pubyear': 1997, 'auth_norm': 0.2, 'bibcode': '1994BoLMe..71..393V', 'cityear': 1994}, 
                                  {'ref_norm': 0.2, 'pubyear': 1997, 'auth_norm': 0.2, 'bibcode': '1994GPC.....9...53M', 'cityear': 1994}, 
                                  {'ref_norm': 0.2, 'pubyear': 1997, 'auth_norm': 0.2, 'bibcode': '1997BoLMe..85...81M', 'cityear': 1997}],
             'rn_citations_hist': {'1994': 0.6000000000000001, '1997': 0.6000000000000001}, 
             'downloads': [], 
             'citation_num': 3, 
             'an_citations': 0.14285714285714285, 
             'refereed_citation_num': 2, 'reads': [], 
             'citations': ['2006QJRMS.132..779R', '2008Sci...320.1622D', '1998PPGeo..22..553A'], 
             'refereed_citations': ['1994BoLMe..71..393V', '1997BoLMe..85...81M'], 
             'author_num': 5, 
             'reference_num': 3, 
             'an_refereed_citations': 0.09523809523809523, 
             'rn_citations': 0.6000000000000001, 
             'id': 3}
        
        p = ProtobufMetricsRecord(**create_clean(m))
        m.pop('id')  # id field should not be in Protobuf
        for key in m:
            if key == 'rn_citation_data':
                for i in range(0, len(m[key])):
                    mm = m[key][i]
                    pp = getattr(p, key)[i]
                    for k in mm:
                        self.compare(mm[k], getattr(pp, k), 'index: {}, key: {}'.format(i, k))
            else:
                self.compare(m[key], getattr(p, key), key)

        m_list = [m, m]
        p_list = ProtobufMetricsRecordList(metrics_records=m_list)
        self.assertEqual(len(m_list), len(p_list.metrics_records))     
        
        
        
    def compare(self, v1, v2, message):     
        """allow floats to be slightly different while other values match exactly"""  
        if isinstance(v1, float):
            self.assertAlmostEqual(v1, v2, 6, message)
        else:
            self.assertEqual(v1, v2, message)
            
    def test_queue_record(self):
        pass

if __name__ == '__main__':
    unittest.main(verbosity=2)

