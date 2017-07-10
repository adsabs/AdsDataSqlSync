import os, sys
PROJECT_HOME = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(PROJECT_HOME)
import unittest
import math
from mock import Mock 

from adsdata.utils import queue_rows, process_rows

class test_queue(unittest.TestCase):

    def test_queue_rows(self):
        """verify queue function is called the correct number of times"""
        
        # one bibcode per line
        f = 'tests/tests/bibcodesTestSample.txt' 
        bibcode_count = sum(1 for line in open(f))
        # batches of 100 records are queued together
        # compute how many batches should be queued
        queue_count = int(math.ceil(bibcode_count / 100.))
        
        db_table = Mock()
        db_table.get_by_bibcodes.return_value = [{}]
        task = Mock()
        task.delay.return_value = None
        logger = Mock()
        queue_rows(f, db_table, task, logger)
        self.assertEqual(queue_count, db_table.get_by_bibcodes.call_count)
        self.assertEqual(queue_count, task.delay.call_count)
        
        
    def test_process_rows_insert(self):
        """verify new records are inserted into database"""
        t = [{'bibcode': 'test1'}, {'bibcode': 'test2'}]
        logger = Mock()
        db_table = Mock()
        # return that record is not in database
        db_table.read.return_value = None 
        process_rows(t, db_table, logger)
        # inserts are performed on individual records
        self.assertEqual(2, db_table.connection.execute.call_count)
        self.assertEqual(2, db_table.table.insert.call_count)

    def test_process_rows_update(self):
        """verify records matching existing bibcodes in database are updated"""
        t = [{'bibcode': 'test1'}, {'bibcode': 'test2'}]
        logger = Mock()
        db_table = Mock()
        # return record is already in database
        db_table.read.return_value = {'id': 1} 
        process_rows(t, db_table, logger)
        # updates are batched, only one should be done
        self.assertEqual(1, db_table.connection.execute.call_count)
        self.assertEqual(0, db_table.table.insert.call_count)
        
        
        
if __name__ == '__main__':
    unittest.main(verbosity=2)
        