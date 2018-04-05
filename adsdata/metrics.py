from sqlalchemy import Column, Integer, Float, String, DateTime, Boolean
from sqlalchemy import Table, bindparam, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema
from sqlalchemy import and_
from sqlalchemy.sql import select
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy import create_engine
from collections import defaultdict
from datetime import datetime
import time
import sys
import json
import argparse

from adsputils import load_config, setup_logging
import nonbib
import models


Base = declarative_base()
meta = MetaData()
metrics_logger = None


class Metrics():
    """computes and provides interface for metrics data"""

    def __init__(self, schema_='metrics'):
        self.logger = setup_logging('AdsDataSqlSync', 'INFO')

        self.schema =  schema_
        self.table = models.MetricsTable()
        self.table.schema = self.schema

        # used to buffer writes                                                                                         
        self.upserts = []
        self.tmp_update_buffer = []
        self.tmp_count = 0
        self.config = {}
        self.config.update(load_config())


    def create_metrics_table(self, db_engine):
        db_engine.execute(CreateSchema(self.schema))
        table = models.MetricsTable()
        table.__table__.schema = self.schema
        table.__table__.create(db_engine)
        self.logger.info('metrics.py, metrics table created')

    def drop_metrics_table(self, db_engine):
        db_engine.execute("drop schema if exists {} cascade".format(self.schema))
        #temp_meta = MetaData()
        #table = self.get_metrics_table(temp_meta)
        #temp_meta.drop_all(self.engine)
        #self.engine.execute(DropSchema(self.schema))
        self.logger.info('metrics.py, metrics table dropped')



    def save(self, db_conn, values):
        """buffered save does actual save every 100 records, call flush at end of processing"""

        bibcode = values.bibcode
        if bibcode is None:
            print 'error: cannont save metrics data that does not have a bibcode'
            return

        self.upserts.append(values)

        self.tmp_count += 1
        if (self.tmp_count % 100) == 0:
            self.flush(db_conn)
            self.tmp_count = 0


    def flush(self, db_conn):
        """bulk write records to sql database"""

        Session = sessionmaker()
        sess = Session(bind=db_conn)
        sess.execute('set search_path to {}'.format('metrics'))
        #sess.bulk_save_objects(self.upserts)
        for current in self.upserts:
            sess.add(current)
        sess.commit()
        sess.close()
        self.upserts = []


    def read(self, db_conn, bibcode):
        """read the passed bibcode from the postgres database"""
        s = select([self.table]).where(self.table.c.bibcode == bibcode)
        r = db_conn.execute(s)
        first = r.first()
        return first

    def get_by_bibcode(self, session, bibcode):
        first = session.query(models.MetricsTable).filter(models.MetricsTable.bibcode==bibcode).first()
        return first


    def get_by_bibcodes(self, session, bibcodes):
        """ return a list of metrics datbase objects matching the list of passed bibcodes"""
        query = session.query(models.MetricsTable).filter(models.MetricsTable.bibcode.in_(bibcodes))
        results = query.all()
        return results


    def update_metrics_changed(self, db_conn, nonbib_conn, row_view_schema='ingest'):  
        """changed bibcodes are in sql table, for each we update metrics record"""
        Nonbib_Session = sessionmaker(bind=nonbib_conn)
        nonbib_sess = Nonbib_Session()
        nonbib_sess.execute('set search_path to {}'.format(row_view_schema))

        Metrics_Session = sessionmaker()
        metrics_sess = Metrics_Session(bind=db_conn)
        metrics_sess.execute('set search_path to {}'.format('metrics'))
        
        sql_sync = nonbib.NonBib(row_view_schema)
        query = nonbib_sess.query(models.NonBibTable)
        count = 0
        for delta_row in nonbib_sess.query(models.NonBibDeltaTable).yield_per(100):
            row = sql_sync.get_by_bibcode(nonbib_conn, delta_row.bibcode)
            metrics_old = metrics_sess.query(models.MetricsTable).filter(models.MetricsTable.bibcode == delta_row.bibcode).first()
            metrics_new = self.row_view_to_metrics(row, nonbib_conn, row_view_schema, metrics_old)
            if metrics_old:
                metrics_sess.merge(metrics_new)
            else:
                metrics_sess.add(metrics_new)
            metrics_sess.commit()

            if (count % 10000) == 0:
                self.logger.debug('delta count = {}, bibcode = {}'.format(count, delta_row.bibcode))
            count += 1
        nonbib_sess.close()
        metrics_sess.close()
        self.flush(db_conn)
        # need to close?

    def update_metrics_bibcode(self, bibcode, nonbib):  #, delta_schema='delta'):
        """changed bibcodes are in sql table, for each we update metrics record"""
        row = nonbib.get_by_bibcode(bibcode)
        metrics_dict = self.row_view_to_metrics(row, sql_sync)
        self.save(metrics_dict)


    # paper from 1988: 1988PASP..100.1134B
    # paper with 3 citations 2015MNRAS.447.1618S
    def update_metrics_test(self, bibcode, row_view_schema='ingest'):
        sql_sync = nonbib.NonBib(row_view_schema)
        row_view_bibcode = sql_sync.get_by_bibcode(bibcode)
        metrics_dict = self.row_view_to_metrics(row_view_bibcode, sql_sync)
        self.save(metrics_dict)
        self.flush()

    def to_sql(self, metrics_dict):
        """return string representation of metrics data suitable for postgres copy from program"""
        return_str = str(metrics_dict['id']) + '\t' + metrics_dict['bibcode']
        return_str += '\t' + str(metrics_dict['refereed'])
        return_str += '\t' + str(metrics_dict['rn_citations'])
        return_str += '\t' + json.dumps(metrics_dict['rn_citation_data'])
        return_str += '\t' + json.dumps(metrics_dict['rn_citations_hist'])
        return_str += '\t' + '{' + str(metrics_dict['downloads']).strip('[]') + '}'
        return_str += '\t' + '{' + str(metrics_dict['reads']).strip('[]') + '}'
        return_str += '\t' + str(metrics_dict['an_citations'])
        return_str += '\t' + str(metrics_dict['refereed_citation_num'])
        return_str += '\t' + str(metrics_dict['citation_num'])
        return_str += '\t' + str(metrics_dict['reference_num'])
        return_str += '\t' + '{' + json.dumps(metrics_dict['citations']).strip('[]') + '}'
        return_str += '\t' + '{' + json.dumps(metrics_dict['refereed_citations']).strip('[]') + '}'
        return_str += '\t' + str(metrics_dict['author_num'])
        return_str += '\t' + str(metrics_dict['an_refereed_citations'])
        return_str += '\t' + str(datetime.now())
        return return_str


    def update_metrics_all(self, db_conn, nonbib_conn, row_view_schema='ingest', start_offset=1, end_offset=-1):
        """update all elements in the metrics database between the passed id offsets"""
        # we request one block of rows from the database at a time
        start_time = time.time()
        step_size = 1000
        count = 0
        offset = start_offset
        max_rows = self.config['MAX_ROWS']
        sql_sync = nonbib.NonBib(row_view_schema)
        Session = sessionmaker(bind=nonbib_conn)
        session = Session()
        session.execute('set search_path to {}'.format(row_view_schema))
        for current_row in session.query(models.NonBibTable).yield_per(100):
            metrics_dict = self.row_view_to_metrics(current_row, nonbib_conn, row_view_schema)
            self.save(db_conn, metrics_dict)
            count += 1
            if max_rows > 0 and count > max_rows:
                break
            if count % 1000 == 0:
                self.logger.debug('metrics.py, metrics count = {}'.format(count))
        self.flush(db_conn)
        end_time = time.time()


    # normalized citations:
    #  for a list of N papers (the citations?)
    #  a = number of authors for the publication
    #  c = number of citations tha paper received (why not call it references?)
    #  c/a = normalized citations
    #  sum over N papers
    def row_view_to_metrics(self, passed_row_view, nonbib_db_conn, row_view_schema='nonbib', m=None):
        """convert the passed row view into a complete metrics dictionary"""
        if m is None:
            m = models.MetricsTable()            
        # first do easy fields
        bibcode = passed_row_view.bibcode
        m.bibcode = bibcode
        m.refereed = passed_row_view.refereed
        m.citations = passed_row_view.citations
        m.reads = passed_row_view.reads
        m.downloads = passed_row_view.downloads

        m.citation_num = len(passed_row_view.citations) if passed_row_view.citations else 0
        m.author_num = max(len(passed_row_view.authors),1) if passed_row_view.authors else 1
        m.reference_num = len(passed_row_view.reference) if passed_row_view.reference else 0

        #metrics_dict['citation_num'] = len(passed_row_view.get('citations', [])
        #metrics_dict['author_num'] = max(len(passed_row_view.get('authors'),[]),1)
        #metrics_dict['reference_num'] = len(passed_row_view.get('reference'),[])

        # next deal with papers that cite the current one
        # compute histogram, normalized values of citations
        #  and create list of refereed citations
        citations = passed_row_view.citations
        normalized_reference = 0.0
        citations_json_records = []
        refereed_citations = []
        citations_histogram = defaultdict(float)
        total_normalized_citations = 0.0
        if citations:
            q = 'select refereed,array_length(reference,1),bibcode from ' + row_view_schema + \
                '.RowViewM where bibcode in (select unnest(citations) from ' + row_view_schema + \
                '.RowViewM where bibcode=%s);'
            result = nonbib_db_conn.execute(q,bibcode)
            for row in result:
                citation_refereed = row[0] if row[0] else False
                citation_refereed = citation_refereed in (True, 't', 'true')
                len_citation_reference = int(row[1]) if row[1] else 0
                citation_bibcode = row[2]

                citation_normalized_references = 1.0 / float(max(5, len_citation_reference))
                total_normalized_citations += citation_normalized_references
                normalized_reference += citation_normalized_references
                tmp_json = {"bibcode":  citation_bibcode.encode('utf-8'),
                            "ref_norm": citation_normalized_references,
                            "auth_norm": 1.0 / m.author_num,
                            "pubyear": int(bibcode[:4]),
                            "cityear": int(citation_bibcode[:4])}
                citations_json_records.append(tmp_json)
                if (citation_refereed):
                    refereed_citations.append(citation_bibcode)
                citations_histogram[citation_bibcode[:4]] += total_normalized_citations

        m.refereed_citations = refereed_citations
        m.refereed_citation_num = len(refereed_citations)

        # annual citations
        today = datetime.today()
        resource_age = max(1.0, today.year - int(bibcode[:4]) + 1)
        m.an_citations = float(m.citation_num) / float(resource_age)
        m.an_refereed_citations = float(m.refereed_citation_num) / float(resource_age)

        # normalized info
        m.rn_citations = normalized_reference     # total_normalized_citations
        m.rn_citation_data = citations_json_records
        m.rn_citations_hist = dict(citations_histogram)
        m.modtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return m

    @staticmethod
    def metrics_mismatch(bibcode, m1, m2, metrics_logger):
        """test function to compare metric records from two different databases"""
        if m1 == None and m2 == None:
            metrics_logger.warn('{} not found in either database'.format(bibcode))
            return ['BibcodeNotFound:' + bibcode]
        elif m1 == None:
            metrics_logger.warn('{} not found in the first database'.format(bibcode))
            return ['BibcodeNotFoundFirstDatabase:' + bibcode]
        elif m2 == None:
            metrics_logger.warn('{} not found in the second database'.format(bibcode))
            return ['BibcodeNotFoundSecondDatabase:' + bibcode]

        mismatches = []
        fields = ('refereed', 'rn_citations', 'rn_citation_data', 'downloads',
                  'reads', 'an_citations', 'refereed_citation_num', 'citation_num',
                  'reference_num', 'citations', 'refereed_citations', 'author_num',
                  'an_refereed_citations', 'rn_citations_hist')
        for field in fields:
            if Metrics.field_mismatch(bibcode, field, m1, m2, metrics_logger):
                metrics_logger.warn('{} mismatch on {}'.format(bibcode, field))
                mismatches.append(field)

        if len(mismatches) > 0:
            return mismatches
        return False;

    @staticmethod
    def field_mismatch(bibcode, fieldname, m1, m2, metrics_logger):
        """test function to compare a field in two different metrics dictionaries"""
        v1 = getattr(m1, fieldname)
        v2 = getattr(m2, fieldname)
        # metrics database has sql null for some reads and downloads while new metrics has array of 0 values
        if v2 == None and v1 == [] or v1 == [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]:
            return False

        if type(v1) != type(v2):
            metrics_logger.warn('{} field type mismatch: {} {}'.format(type(v1), type(v2)))
            return True

        if v1 == None and v2 == None:
            return False

        if fieldname in ('downloads', 'reads'):
            # only last value may be different to account for slightly older test data
            t1 = v1[:-1]
            t2 = v2[:-1]
            for x1,x2 in zip(t1, t2):
                if x1 != x2:
                    metrics_logger.warn('{} {} arrays differ on not last element: {} {}'.format(bibcode, fieldname, x1, x2))
                    return True;
            t1 = v1[-1]
            t2 = v2[-1]
            delta = abs(t1 - t2)
            limit = max(5, t1 * .15)
            if delta >  limit:
                metrics_logger.warn('{} {} last element of arrays differ: {} {}, threshold: {}'.format(bibcode, fieldname, t1, t2, limit))
                return True
            return False

        # if value is an array, compare individual elements
        if isinstance(v1, list):
            limit = 3
            delta = abs(len(v1) - len(v2))
            if delta > 0 and delta < limit:
                metrics_logger.info('{} {} array elements nearly match, lengh delta: {}'.format(bibcode, fieldname, delta))
            elif delta >= limit:
                metrics_logger.warn('{} {} array elements do not match, length delta {}, threshold: {}'.format(bibcode, fieldname, delta, limit))
                return True

            warning_count = 0
            # compute delta between lists, then report differences
            if fieldname == 'rn_citation_data':
                m1_bibs = {element['bibcode'] for element in v1}
                m2_bibs = {element['bibcode'] for element in v2}
                missing_from_first = m2_bibs.difference(m1_bibs)
                missing_from_second = m1_bibs.difference(m2_bibs)
            else:
                v1_set = set(v1)
                v2_set = set(v2)
                missing_from_first = v2_set.difference(v1_set)
                missing_from_second = v1_set.difference(v2_set)

            if len(missing_from_first):
                warning_count += len(missing_from_first)
                for v in missing_from_first:
                    metrics_logger.warn('{} {} list element missing from first list: {}'.format(bibcode, fieldname, v))
            if len(missing_from_second):
                warning_count += len(missing_from_second)
                for v in missing_from_second:
                    metrics_logger.warn('{} {} list element missing from second list: {}'.format(bibcode, fieldname, v))
            if warning_count > 3:
                metrics_logger.warn('{} {} list element mismatch over threshold of 3'.format(bibcode, fieldname))
                return True

            return False

        # otherwise, see if scalar values match
        return Metrics.value_mismatch(bibcode, fieldname, v1, v2, metrics_logger)

    @staticmethod
    def value_mismatch(bibcode, fieldname, v1, v2, metrics_logger):
        """test function to compare to scalar values from two different metrics dictionaries"""
        if type(v1) != type(v2):
            metrics_logger.warn('{} {} type values mismatch: {} {}'.format(bibcode, fieldname, type(v1), type(v2)))
            return True

        if v1 == None or v2 == None:
            if v1 == None and v2 == None: return False
            metrics_logger.warn('{} {} None value mismatch {} {}'.format(bibcode, fieldname, v1, v2))
            return True

        if isinstance(v1,(str, unicode)):
            mismatch = v1 != v2
            if mismatch:
                metrics_logger.warn('{} {} string values mismatch: {} {} '.format(bibcode, fieldname, v1, v2))
            return mismatch

        if isinstance(v1, bool):
            if v1 != v2:
                metrics_logger.warn('{} {} boolean values mismatch {} {}'.format(bibcode, fieldname, v1, v2))
            return v1 != v2

        if isinstance(v1, (int, float)):
            delta = abs(v1 - v2)
            limit = max(abs(v1), abs(v2)) * .1
            if (delta > limit):
                metrics_logger.warn('{} {} float values mismatch {} {}, limit {}'.format(bibcode, fieldname, v1, v2, limit))
                return True
            else:
                return False
        if isinstance(v1, dict):
            k1 = v1.keys()
            k2 = v2.keys()
            if len(k1) != len(k2):
                metrics_logger.warn('{} {} dict number of keys mismatch {} {}'.format(bibcode, fieldname, v1.keys(), v2.keys()))
                return True
            for k in k1:
                if v1[k] != v2[k]:
                    metrics_logger.warn('{} {} dict values for key {} mismatch {} {}'.format(bibcode, fieldname, k, v1[k], v2[k]))
                    return True
            return False
        raise ValueError('Unexpected data type passed to value_mismatch,type = ' + str(type(v1)))


