
import os, sys
PROJECT_HOME = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(PROJECT_HOME)
import unittest
from adsputils import load_config, setup_logging
from adsdata import reader
from run import is_article, cleanup_for_master

class test_run(unittest.TestCase):
    """currently, run.py has too much code but we test it in place for now"""

    def setup(self):
        pass

    def test_is_article(self):
        self.assertFalse(is_article(['foo']))
        self.assertTrue(is_article(['eprint'])) 
        self.assertTrue(is_article(['foo', 'eprint'])) 
        self.assertTrue(is_article(['eprint', 'bar'])) 
        self.assertTrue(is_article(['foo', 'eprint', 'bar'])) 
        self.assertTrue(is_article(['eprint', 'inbook'])) 
        self.assertTrue(is_article(['article']))  
        self.assertTrue(is_article(['inproceedings'])) 
        self.assertTrue(is_article(['inbook'])) 

    def test_cleanup_for_master(self):
        x = {'refereed':True, 'foo':'bar'}
        cleanup_for_master(x)
        self.assertFalse('refereed' in x)
        self.assertTrue('foo' in x)



if __name__ == '__main__':
    unittest.main(verbosity=2)
