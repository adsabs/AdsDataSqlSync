
import os, sys
PROJECT_HOME = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(PROJECT_HOME)
import unittest
from mock import Mock
from adsputils import load_config, setup_logging
from adsdata import reader
from adsdata.tasks import _cleanup_for_master

class test_run(unittest.TestCase):
    """currently, run.py has too much code but we test it in place for now"""

    def setup(self):
        pass


    def test_cleanup_for_master(self):
        x = {'refereed':True, 'foo':'bar'}
        _cleanup_for_master(x)
        self.assertFalse('refereed' in x)
        self.assertTrue('foo' in x)

if __name__ == '__main__':
    unittest.main(verbosity=2)
