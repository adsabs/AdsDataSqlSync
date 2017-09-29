import sys
import os

PROJECT_HOME = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(PROJECT_HOME)
print PROJECT_HOME

import unittest
import psycopg2
import testing.postgresql

from adsputils import load_config
from run import fetch_data_link_elements, fetch_data_link_elements_counts

class test_resolver(unittest.TestCase):
    """tests for generation of resolver"""


    # Reference to testing.postgresql database instance
    db = None

    # Connection to the database used to set the database state before running each
    # test
    db_con = None

    # Map of database connection parameters passed to the functions we're testing
    db_conf = None

    config = {}
    config.update(load_config())

    def setUp(self):
        """ Module level set-up called once before any tests in this file are
        executed.  Creates a temporary database and sets it up """
        global db, db_con, db_conf
        db = testing.postgresql.Postgresql()
        # Get a map of connection parameters for the database which can be passed
        # to the functions being tested so that they connect to the correct
        # database
        db_conf = db.dsn()
        # Create a connection which can be used by our test functions to set and
        # query the state of the database
        db_con = psycopg2.connect(**db_conf)
        # Commit changes immediately to the database
        db_con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        with db_con.cursor() as cur:
            # Create the initial database structure (roles, schemas, tables etc.)
            # basically anything that doesn't change
            cur.execute(self.slurp('tests/data/datalinks.sql'))

    def tearDown(self):
        """ Called after all of the tests in this file have been executed to close
        the database connecton and destroy the temporary database """
        db_con.close()
        db.stop()


    def slurp(self, path):
        """ Reads and returns the entire contents of a file """
        with open(path, 'r') as f:
            return f.read()


    def test_data_query(self):
        with db_con.cursor() as cur:
            cur.execute(self.config['DATA_QUERY'].format(db='public', bibcode='1903BD....C......0A'))
            self.assertEqual(fetch_data_link_elements_counts(cur.fetchone()), [['CDS:1', 'Vizier:1'], 2])


    def test_esource_query1(self):
        with db_con.cursor() as cur:
            cur.execute(self.config['ESOURCE_QUERY'].format(db='public', bibcode='2016Atoms...4...18I'))
            self.assertEqual(fetch_data_link_elements(cur.fetchone()), ['EPRINT_HTML', 'EPRINT_PDF'])


    def test_esource_query2(self):
        with db_con.cursor() as cur:
            cur.execute(self.config['ESOURCE_QUERY'].format(db='public', bibcode='2014MNRAS.444.1496E'))
            self.assertEqual(fetch_data_link_elements(cur.fetchone()), ['PUB_PDF'])


    def test_esource_query3(self):
        with db_con.cursor() as cur:
            cur.execute(self.config['ESOURCE_QUERY'].format(db='public', bibcode='2014MNRAS.444.1497S'))
            self.assertEqual(fetch_data_link_elements(cur.fetchone()), ['EPRINT_HTML', 'EPRINT_PDF', 'PUB_PDF'])


    def test_property_query1(self):
        with db_con.cursor() as cur:
            cur.execute(self.config['PROPERTY_QUERY'].format(db='public', bibcode='2004MNRAS.354L..31M'))
            self.assertEqual(fetch_data_link_elements(cur.fetchone()), ['ARTICLE', 'ASSOCIATED', 'INSPIRE'])


    def test_property_query2(self):
        with db_con.cursor() as cur:
            cur.execute(self.config['PROPERTY_QUERY'].format(db='public', bibcode='1891opvl.book.....N'))
            self.assertEqual(fetch_data_link_elements(cur.fetchone()), ['LIBRARYCATALOG'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
