import sys
import os

PROJECT_HOME = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(PROJECT_HOME)
print PROJECT_HOME

import unittest
import psycopg2
import testing.postgresql

from adsdata.datalinks import _fetch_data_link_elements, _fetch_data_link_elements_counts, _fetch_data_link_record, _add_data_link_extra_properties
from adsdata.datalinks import PROPERTY_QUERY, ESOURCE_QUERY, DATA_QUERY, DATALINKS_QUERY

class test_resolver(unittest.TestCase):
    """tests for generation of resolver"""


    # Reference to testing.postgresql database instance
    db = None

    # Connection to the database used to set the database state before running each
    # test
    db_con = None

    # Map of database connection parameters passed to the functions we're testing
    db_conf = None

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
        self.maxDiff = None

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
            cur.execute(DATA_QUERY.format(db='public', bibcode='1903BD....C......0A'))
            self.assertEqual(_fetch_data_link_elements_counts(cur.fetchone()), [['CDS:1', 'Vizier:1'], 2])


    def test_esource_query1(self):
        with db_con.cursor() as cur:
            cur.execute(ESOURCE_QUERY.format(db='public', bibcode='2016Atoms...4...18I'))
            self.assertEqual(_fetch_data_link_elements(cur.fetchone()), ['EPRINT_HTML', 'EPRINT_PDF'])


    def test_esource_query2(self):
        with db_con.cursor() as cur:
            cur.execute(ESOURCE_QUERY.format(db='public', bibcode='2014MNRAS.444.1496E'))
            self.assertEqual(_fetch_data_link_elements(cur.fetchone()), ['PUB_PDF'])


    def test_esource_query3(self):
        with db_con.cursor() as cur:
            cur.execute(ESOURCE_QUERY.format(db='public', bibcode='2014MNRAS.444.1497S'))
            self.assertEqual(_fetch_data_link_elements(cur.fetchone()), ['EPRINT_HTML', 'EPRINT_PDF', 'PUB_PDF'])


    def test_property_query1(self):
        with db_con.cursor() as cur:
            cur.execute(PROPERTY_QUERY.format(db='public', bibcode='2004MNRAS.354L..31M'))
            self.assertEqual(_fetch_data_link_elements(cur.fetchone()), ['ASSOCIATED', 'ESOURCE', 'INSPIRE'])


    def test_property_query2(self):
        with db_con.cursor() as cur:
            cur.execute(PROPERTY_QUERY.format(db='public', bibcode='1891opvl.book.....N'))
            self.assertEqual(_fetch_data_link_elements(cur.fetchone()), ['LIBRARYCATALOG'])

    def test_property_query3(self):
        with db_con.cursor() as cur:
            cur.execute(PROPERTY_QUERY.format(db='public', bibcode='2018LPI....49.2177B'))
            self.assertEqual(_fetch_data_link_elements(cur.fetchone()), ['ESOURCE', 'TOC'])

    def test_extra_property_values(self):
        current_row = {}
        extra_properties = [{'pub_openaccess':True, 'private':False, 'ocrabstract':False,
                             'nonarticle':True, 'refereed':True},
                            {'pub_openaccess':False, 'private':True, 'ocrabstract':True,
                             'nonarticle':False, 'refereed':False}]
        results = [['NONARTICLE', 'REFEREED', 'PUB_OPENACCESS', 'ADS_OPENACCESS', 'AUTHOR_OPENACCESS', 'EPRINT_OPENACCESS', 'OPENACCESS'],
                   ['ARTICLE', 'NOT REFEREED', 'PRIVATE', 'OCRABSTRACT']]
        esources = [['ADS_PDF', 'AUTHOR_PDF', 'EPRINT_HTML'],
                   []]
        for extra_property, result, esource in zip(extra_properties, results, esources):
            current_row['property'] = []
            for key, value in extra_property.iteritems():
                current_row[key] = value
            current_row['esource'] = esource
            current_row = _add_data_link_extra_properties(current_row)
            self.assertEqual(current_row['property'], result)

    def test_datalinks_query(self):
        with db_con.cursor() as cur:
            cur.execute(DATALINKS_QUERY.format(db='public', bibcode='2004MNRAS.354L..31M'))
            rec = _fetch_data_link_record(cur.fetchall())
            expected = [{'url': ['http://articles.adsabs.harvard.edu/pdf/1825AN......4..241B'],
                         'title': [], 'item_count': 0, 'link_type': 'ESOURCE',
                         'link_sub_type': 'ADS_PDF'},
                        {'url': ['1825AN......4..241B', '2010AN....331..852K'],
                         'title': ['Main Paper', 'Translation'], 'item_count': 0,
                         'link_type': 'ASSOCIATED', 'link_sub_type': 'NA'},
                        {'url': [], 'title': [], 'item_count': 0, 'link_type': 'INSPIRE',
                         'link_sub_type': 'NA'}]
            self.assertEqual(rec, expected)

    def test_datalinks_query_for_associated(self):
        with db_con.cursor() as cur:
            cur.execute(DATALINKS_QUERY.format(db='public', bibcode='2004MNRAS.354L..31M'))
            self.assertEqual(_fetch_data_link_record(cur.fetchall()), [{'url': ['http://articles.adsabs.harvard.edu/pdf/1825AN......4..241B'], 'title': [], 'item_count': 0, 'link_type': 'ESOURCE', 'link_sub_type': 'ADS_PDF'},
                                                                      {'url': ['1825AN......4..241B', '2010AN....331..852K'], 'title': ['Main Paper', 'Translation'], 'item_count': 0, 'link_type': 'ASSOCIATED', 'link_sub_type': 'NA'},
                                                                      {'url': [], 'title': [], 'item_count': 0, 'link_type': 'INSPIRE', 'link_sub_type': 'NA'}])

if __name__ == '__main__':
    unittest.main(verbosity=2)
