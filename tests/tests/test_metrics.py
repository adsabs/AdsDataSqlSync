
import sys
import os
PROJECT_HOME = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(PROJECT_HOME)

import unittest
from datetime import datetime
from mock import Mock, patch
from adsdata.metrics import Metrics
from adsdata.models import NonBibTable

class metrics_test(unittest.TestCase):

    """tests for generation of metrics database"""

    t1 = NonBibTable()
    t1.bibcode = "1998PPGeo..22..553A"
    t1.refereed = False 
    t1.authors = ["Arnfield, A. L."]
    t1.downloads = []
    t1.reads = [1, 2, 3, 4]
    t1.downloads = [0, 1, 2, 3]
    t1.citations =  []
    t1.id = 11
    t1.reference = ["1997BoLMe..85..475M"]

    t2 = NonBibTable()
    t2.bibcode = "1997BoLMe..85..475M"
    t2.refereed = True
    t2.authors = ["Meesters, A. G. C. A.", "Bink, N. J.",  "Henneken, E. A. C.", "Vugts, H. F.", "Cannemeijer, F."]
    t2.downloads = []
    t2.reads = []
    t2.citations = ["2006QJRMS.132..779R", "2008Sci...320.1622D", "1998PPGeo..22..553A"]
    t2.id = 3
    t2.reference = ["1994BoLMe..71..393V", "1994GPC.....9...53M", "1997BoLMe..85...81M"]

    test_data = [t1, t2]

    def setUp(self):
        # perhaps not useful with only two sample data sets
        self.no_citations = [x for x in metrics_test.test_data if not x.citations]
        self.citations = [x for x in metrics_test.test_data if x.citations]


    def test_trivial_fields(self):
        """test fields that are not transformed"""

        with patch('sqlalchemy.create_engine'):
            met = Metrics() 
            for record in self.no_citations:
                metrics_dict = met.row_view_to_metrics(record, None)
                self.assertEqual(record.bibcode, metrics_dict.bibcode, 'bibcode check')
                self.assertEqual(record.citations, metrics_dict.citations, 'citations check')
                self.assertEqual(record.reads, metrics_dict.reads, 'reads check')
                self.assertEqual(record.downloads, metrics_dict.downloads, 'downloads check')

    def test_num_fields(self):
        """test fields based on length of other fields"""
                
        with patch('sqlalchemy.create_engine'):
            met = Metrics()
            for record in self.no_citations:
                metrics_dict = met.row_view_to_metrics(record, None)
                self.assertEqual(metrics_dict.citation_num, len(record.citations), 'citation number check')
                self.assertEqual(metrics_dict.reference_num, len(record.reference), 'reference number check')
                self.assertEqual(metrics_dict.author_num, len(record.authors), 'author number check')
                self.assertEqual(metrics_dict.refereed_citation_num, 0, 'refereed citation num')

    def test_with_citations(self):
        """test a bibcode that has citations"""

        test_row = metrics_test.t2 
        t2_year = int(metrics_test.t2.bibcode[:4])
        today = datetime.today()
        t2_age = max(1.0, today.year - t2_year + 1) 

        # we mock row view select for citation data with hard coded results
        #   for row_view_to_metrics to use ([refereed, len(reference), bibcode], ...)
        m = Mock()
        m.schema = "None"
        m.execute.return_value = (
            [True, 1, "1994BoLMe..71..393V"],
            [False, 1, "1994GPC.....9...53M"],
            [True, 1, "1997BoLMe..85...81M"])


        with patch('sqlalchemy.create_engine'):        
            met = Metrics()
            metrics_dict = met.row_view_to_metrics(metrics_test.t2, m)
            self.assertEqual(len(metrics_dict.citations), 3, 'citations check')
            self.assertEqual(len(metrics_dict.refereed_citations), 2, 'refereed citations check')
            self.assertEqual(metrics_dict.refereed_citations[0], "1994BoLMe..71..393V", 'refereed citations check')
            self.assertEqual(metrics_dict.refereed_citations[1], "1997BoLMe..85...81M", 'refereed citations check')
            rn_citation_data_0 = {'ref_norm': 0.2, 'pubyear': 1997, 'auth_norm': 0.2, 
                                  'bibcode': '1994BoLMe..71..393V', 'cityear': 1994}
            self.assertEqual(metrics_dict.rn_citation_data[0], rn_citation_data_0, 'rn citation data')
            self.assertAlmostEqual(metrics_dict.an_refereed_citations, 2. / t2_age, 5, 'an refereed citations')
            self.assertAlmostEqual(metrics_dict.rn_citations, .6, 5, 'rn citations')

    def test_validate_lists(self):
        """test validation code for lists

        send both matching and mismatching data to metrics list validation and verify correct responses"""

        # base data
        rn_citation_data1 = Mock()
        rn_citation_data1.refereed_citations = ["2015MNRAS.447.1618S", "2016MNRAS.456.1886S", "2015MNRAS.451..149J"]
        rn_citation_data1.rn_citation_data = [{"ref_norm": 0.0125, "pubyear": 2014, "auth_norm": 0.3333333333333333, 
                                               "bibcode": "2015MNRAS.447.1618S", "cityear": 2015}, 
                                              {"ref_norm": 0.012048192771084338, "pubyear": 2014, "auth_norm": 0.3333333333333333, 
                                               "bibcode": "2016MNRAS.456.1886S", "cityear": 2016}, 
                                              {"ref_norm": 0.02702702702702703, "pubyear": 2014, "auth_norm": 0.3333333333333333, 
                                               "bibcode": "2015MNRAS.451..149J", "cityear": 2015}]

        # only slightly different than base, should not be a mismatch
        rn_citation_data1a = Mock()
        rn_citation_data1a.refereed_citations = ["2015MNRAS.447.1618S", "2016MNRAS.456.1886S", "2015MNRAS.451..149Z"]
        rn_citation_data1a.rn_citation_data = [{"ref_norm": 0.0125, "pubyear": 2014, "auth_norm": 0.3333333333333333, 
                                               "bibcode": "2015MNRAS.447.1618Z", "cityear": 2015}, 
                                              {"ref_norm": 0.012048192771084338, "pubyear": 2014, "auth_norm": 0.3333333333333333, 
                                               "bibcode": "2016MNRAS.456.1886S", "cityear": 2016}, 
                                              {"ref_norm": 0.02702702702702703, "pubyear": 2014, "auth_norm": 0.3333333333333333, 
                                               "bibcode": "2015MNRAS.451..149J", "cityear": 2015}]

        # very different from base, should be a mismatch
        rn_citation_data2 = Mock()
        rn_citation_data2.refereed_citations = ["2015MNRAS.447.1618Z", "2016MNRAS.456.1886Z", "2015MNRAS.451..149Z", "2015MNRAS.451..149Y"]
        rn_citation_data2.rn_citation_data = [{"ref_norm": 0.0125, "pubyear": 2014, "auth_norm": 0.3333333333333333, 
                                               "bibcode": "2015MNRAS.447.1618Z", "cityear": 2015}, 
                                              {"ref_norm": 0.012048192771084338, "pubyear": 2014, "auth_norm": 0.3333333333333333, 
                                               "bibcode": "2016MNRAS.456.1886Z", "cityear": 2016}, 
                                              {"ref_norm": 0.02702702702702703, "pubyear": 2014, "auth_norm": 0.3333333333333333, 
                                               "bibcode": "2015MNRAS.451..149Z", "cityear": 2015},
                                              {"ref_norm": 0.02702702702702703, "pubyear": 2014, "auth_norm": 0.3333333333333333, 
                                               "bibcode": "2015MNRAS.451..149Y", "cityear": 2015}]

        logger = Mock()
        mismatch = Metrics.field_mismatch('2014AJ....147..124M', 'rn_citation_data', rn_citation_data1, rn_citation_data1, logger)
        self.assertFalse(mismatch, 'validate rn_citation_data')
        mismatch = Metrics.field_mismatch('2014AJ....147..124M', 'rn_citation_data', rn_citation_data1, rn_citation_data2, logger)
        self.assertTrue(mismatch, 'validate rn_citation_data')

        mismatch = Metrics.field_mismatch('2014AJ....147..124M', 'rn_citation_data', rn_citation_data1, rn_citation_data1a, logger)
        self.assertFalse(mismatch, 'validate rn_citation_data')
        mismatch = Metrics.field_mismatch('2014AJ....147..124M', 'refereed_citations', rn_citation_data1a, rn_citation_data1a, logger)
        self.assertFalse(mismatch, 'validate refereed_citations')

        mismatch = Metrics.field_mismatch('2014AJ....147..124M', 'refereed_citations', rn_citation_data1, rn_citation_data1, logger)
        self.assertFalse(mismatch, 'validate refereed_citations')
        mismatch = Metrics.field_mismatch('2014AJ....147..124M', 'refereed_citations', rn_citation_data1, rn_citation_data2, logger)
        self.assertTrue(mismatch, 'validate refereed_citations')

        
if __name__ == '__main__':
    unittest.main(verbosity=2)
