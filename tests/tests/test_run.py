
import os, sys
PROJECT_HOME = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(PROJECT_HOME)
import unittest
from mock import Mock
from adsputils import load_config, setup_logging
from adsdata import reader
from run import cleanup_for_master, nonbib_to_master_dict

class test_run(unittest.TestCase):
    """currently, run.py has too much code but we test it in place for now"""

    def setup(self):
        pass


    def test_cleanup_for_master(self):
        x = {'refereed':True, 'foo':'bar'}
        cleanup_for_master(x)
        self.assertFalse('refereed' in x)
        self.assertTrue('foo' in x)

    def test_nonbib_to_master_dict(self):
        row = Mock()
        row.authors = ()
        row.citation_count = 5
        row.bibcode = 'testbib'
        d = nonbib_to_master_dict(row)
        self.assertEqual(1, d['citation_count_norm'])
        self.assertEqual('testbib', d['bibcode'])
        
        row.authors = ['foo', 'bar']
        d = nonbib_to_master_dict(row)
        self.assertAlmostEqual(5/2., d['citation_count_norm'], places=5)

if __name__ == '__main__':
    unittest.main(verbosity=2)
