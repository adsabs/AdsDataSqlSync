from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.schema import CreateSchema, DropSchema

from sqlalchemy import *
from sqlalchemy.sql import select
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import sessionmaker
from collections import defaultdict
from datetime import datetime
import time
import sys
import json
import argparse
import logging

import utils

sys.path.append('/SpacemanSteve/code/cfa/AdsDataSqlSync/AdsDataSqlSync/sqlSync')
from row_view import SqlSync

from settings import(ROW_VIEW_DATABASE, METRICS_DATABASE, METRICS_DATABASE2)

Base = declarative_base()
meta = MetaData()

# part of the Metrics system

class Metrics():

    def __init__(self, metrics_schema='metrics', passed_config=None):
        #metrics_database='postgresql://postgres@localhost:5432/postgres'
        #         from_scratch=False, copy_from_program=False):
        self.config = {}
        self.config.update(utils.load_config())
        if passed_config:
            self.config.update(passed_config)

        self.meta = MetaData()
        self.schema = metrics_schema
        self.table = self.get_metrics_table()
        self.database = self.config.get('INGEST_DATABASE', 'postgresql://postgres@localhost:5432/postgres')
        self.engine = create_engine(self.database)
        self.connection = self.engine.connect()
        # if true, don't bother checking if metrics database has bibcode, assume db insert
        self.from_scratch = self.config.get('FROM_SCRATCH', True)
        # if true, send data to stdout
        self.copy_from_program = self.config.get('COPY_FROM_PROGRAM', True)
        self.upserts = []

        # sql command to update a row in the metrics table
        self.u = self.table.update().where(self.table.c.bibcode == bindparam('tmp_bibcode')). \
            values ({'refereed': bindparam('refereed'),
                     'rn_citations': bindparam('rn_citations'),
                     'rn_citation_data': bindparam('rn_citation_data'),
                     'downloads': bindparam('downloads'),
                     'reads': bindparam('reads'),
                     'an_citations': bindparam('an_citations'),
                     'refereed_citation_num': bindparam('refereed_citation_num'),
                     'citation_num': bindparam('citation_num'),
                     'reference_num': bindparam('reference_num'),
                     'citations': bindparam('citations'),
                     'refereed_citations': bindparam('refereed_citations'),
                     'author_num': bindparam('author_num'),
                     'an_refereed_citations': bindparam('author_num')})

        self.tmp_count = 0
        self.tmp_update_buffer = []
        self.logger = logging.getLogger('AdsDataSqlSync')

    def get_metrics_table(self, meta=None):
        if meta is None:
            meta = self.meta
        return Table('metrics', meta,
                     Column('id', Integer, primary_key=True),
                     Column('bibcode', String, nullable=False, index=True, unique=True),
                     Column('refereed',Boolean),
                     Column('rn_citations', postgresql.REAL),
                     Column('rn_citation_data', postgresql.JSON),
                     # Column('rn_citations_hist', postgresql.JSON),
                     Column('downloads', postgresql.ARRAY(Integer)),
                     Column('reads', postgresql.ARRAY(Integer)),
                     Column('an_citations', postgresql.REAL),
                     Column('refereed_citation_num', Integer),
                     Column('citation_num', Integer),
                     Column('reference_num', Integer),
                     Column('citations', postgresql.ARRAY(String)),
                     Column('refereed_citations', postgresql.ARRAY(String)),
                     Column('author_num', Integer),
                     Column('an_refereed_citations', postgresql.REAL),
                     Column('modtime', DateTime),
                     schema=self.schema)
    #extend_existing=True) 

    def create_metrics_table(self):
        self.engine.execute(CreateSchema(self.schema))
        temp_meta = MetaData()
        table = self.get_metrics_table(temp_meta)
        temp_meta.create_all(self.engine)
        self.logger.info('metrics.py, metrics table created')


    def drop_metrics_table(self):
        temp_meta = MetaData()
        table = self.get_metrics_table(temp_meta)
        temp_meta.drop_all(self.engine)
        self.engine.execute(DropSchema(self.schema))
        self.logger.info('metrics.py, metrics table dropped')



    def test_query(self, bibcode):
        q ='select refereed,array_length(reference,1),bibcode from ingest.RowViewM where bibcode = ANY ((select citations from ingest.RowViewM where bibcode=\'' + bibcode + '\')::text[]);'
        result = sql_sync_engine.execute(q)
        for row in result:
            refereed_flag = row[0]
            len_reference = row[1]
            citation_bibcode = row[2]
            print refereed_flag, len_reference, citation_bibcode, '!!'


    def save(self, values_dict):
        """buffered save does actual save every 100 records, call flush at end of processing"""

        bibcode = values_dict['bibcode']
        if bibcode is None:
            print 'error: cannont save metrics data that does not have a bibcode'
            return
        
        if self.copy_from_program:
            print self.to_sql(values_dict)
            return

        self.upserts.append(values_dict)

        self.tmp_count += 1
        if (self.tmp_count % 100) != 0:
            self.flush()
            self.tmp_count = 0


    def flush(self):
        """bulk write records to sql database"""
        if self.copy_from_program:
            return

        updates = []
        inserts = []
        if self.from_scratch:
            # here if we just assume these records are not in database and insert
            # it may have just been created so we just insert, not update
            inserts = self.upserts
        else:
            # here if we have to check each record to see if we update or insert
            for d in self.upserts:
                current_bibcode = d['bibcode']
                s = select([self.table]).where(self.table.c.bibcode== current_bibcode)
                r = self.connection.execute(s)
                first = r.first()
                if first:
                    d['tmp_bibcode'] = d['bibcode']
                    updates.append(d)
                else:
                    inserts.append(d)

        if len(updates):
            result = self.connection.execute(self.u, updates)
        if len(inserts):
            result = self.connection.execute(self.table.insert(), inserts)

        self.upserts = []

        
    def read(self, bibcode):
        """read the passed bibcode from the postgres database"""
        s = select([self.table]).where(self.table.c.bibcode == bibcode)
        r = self.connection.execute(s)
        first = r.first()
        return first

    def update_metrics_changed(self, row_view_schema='ingest', delta_schema='delta'):
        delta_sync = SqlSync(delta_schema)
        delta_table = delta_sync.get_delta_table()
        sql_sync = SqlSync(row_view_schema)
        row_view = sql_sync.get_row_view_table()
        connection = sql_sync_engine.connect()
        s = select([delta_table])
        results = connection.execute(s)
        for delta_row in results:
            row = sql_sync.get_row_view(delta_row['bibcode'])
            metrics_dict = self.row_view_to_metrics(row_view)
            self.save(metrics_dict)
        self.flush()

    # paper from 1988: 1988PASP..100.1134B
    # paper with 3 citations 2015MNRAS.447.1618S
    def update_metrics_test(self, bibcode, row_view_schema='ingest'):
        sql_sync = SqlSync(row_view_schema)
        row_view = sql_sync.get_row_view(bibcode)
        metrics_dict = self.row_view_to_metrics(row_view, sql_sync)
        self.save(metrics_dict)
        self.flush()

    def to_sql(self, metrics_dict):
        """return string representation of metrics data suitable for postgres copy from program"""
        return_str = str(metrics_dict['id']) + '\t' + metrics_dict['bibcode']
        return_str += '\t' + str(metrics_dict['refereed'])
        return_str += '\t' + str(metrics_dict['rn_citations'])
        return_str += '\t' + json.dumps(metrics_dict['rn_citation_data'])
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


    def update_metrics_all(self, row_view_schema='ingest', start_offset=1, end_offset=-1):
        """update all elements in the metrics database between the passed id offsets"""
        # we request one block of rows from the database at a time
        start_time = time.time()
        step_size = 1000
        count = 0
        offset = start_offset
        sql_sync = SqlSync(row_view_schema)
        table = sql_sync.get_row_view_table()
        while True:
            s = select([table])
            s = s.where(sql_sync.row_view_table.c.id >= offset).where(sql_sync.row_view_table.c.id < offset + step_size)
            connection = sql_sync.sql_sync_connection; 
            results = connection.execute(s)
            for row_view in results:
                metrics_dict = self.row_view_to_metrics(row_view, sql_sync)
                self.save(metrics_dict)
                count += 1
                if count % 100000 == 0:
                    self.logger.debug('metrics.py, metrics count = {}'.format(count))
                if end_offset > 0 and end_offset <= (count + start_offset):
                    # here if we processed the last requested row
                    self.flush()
                    end_time = time.time()
                    return

            if results.rowcount < step_size:
                # here if last read got the last block of data and we're done processing it
                self.flush()
                end_time = time.time()
                return;

            offset += step_size

    # normalized citations:
    #  for a list of N papers (the citations?)
    #  a = number of authors for the publication 
    #  c = number of citations tha paper received (why not call it references?)
    #  c/a = normalized citations
    #  sum over N papers
    def row_view_to_metrics(self, row_view, sql_sync):
        """convert the passed row view into a complete metrics dictionary"""
        # first do easy fields
        bibcode = row_view['bibcode']
        metrics_dict = {'bibcode': bibcode}
        metrics_dict['id'] = row_view['id']
        metrics_dict['refereed'] = row_view['refereed']
        metrics_dict['citations'] = row_view['citations']
        metrics_dict['reads'] = row_view['reads']
        metrics_dict['downloads'] = row_view['downloads']
        
        metrics_dict['citation_num'] = len(row_view['citations']) if row_view['citations'] else 0
        metrics_dict['author_num'] = max(len(row_view['authors']),1) if row_view['authors'] else 1
        metrics_dict['reference_num'] = len(row_view['reference']) if row_view['reference'] else 0

        #metrics_dict['citation_num'] = len(row_view.get('citations', [])
        #metrics_dict['author_num'] = max(len(row_view.get('authors'),[]),1)
        #metrics_dict['reference_num'] = len(row_view.get('reference'),[]) 

        # next deal with papers that cite the current one
        # compute histogram, normalized values of citations
        #  and create list of refereed citations
        citations = row_view['citations']
        normalized_reference = 0.0
        citations_json_records = []
        refereed_citations = []
        total_normalized_citations = 0.0
        if citations:
            q = 'select refereed,array_length(reference,1),bibcode from ' + sql_sync.schema_name + '.RowViewM where bibcode in (select unnest(citations) from ' + sql_sync.schema_name + '.RowViewM where bibcode=%s);'
            result = sql_sync.sql_sync_connection.execute(q,bibcode)
            for row in result:
                citation_refereed = row[0] if row[0] else False
                citation_refereed = citation_refereed in (True, 't', 'true')
                len_citation_reference = int(row[1]) if row[1] else 0
                citation_bibcode = row[2]

                citation_normalized_references = 1.0 / float(max(5, len_citation_reference))
                normalized_reference += citation_normalized_references
                tmp_json = {"bibcode":  citation_bibcode.encode('utf-8'),
                            "ref_norm": citation_normalized_references,
                            "auth_norm": 1.0 / metrics_dict['author_num'],
                            "pubyear": int(bibcode[:4]),
                            "cityear": int(citation_bibcode[:4])}
                citations_json_records.append(tmp_json)
                if (citation_refereed):
                    refereed_citations.append(citation_bibcode)

        metrics_dict['refereed_citations'] = refereed_citations
        metrics_dict['refereed_citation_num'] = len(refereed_citations)

        # annual citations
        today = datetime.today()
        resource_age = max(1.0, today.year - int(bibcode[:4]) + 1) 
        metrics_dict['an_citations'] = float(metrics_dict['citation_num'] / resource_age)
        metrics_dict['an_refereed_citations'] = float(metrics_dict['refereed_citation_num'] / resource_age)

        # normalized info
        metrics_dict['rn_citations'] = normalized_reference     # total_normalized_citations  
        metrics_dict['rn_citation_data'] = citations_json_records
        # metrics_dict['rn_citations_hist'] = citations_histogram
        
        return metrics_dict

    @staticmethod
    def metrics_mismatch(bibcode, metrics1, metrics2):
        """test function to compare metric records from two different databases"""
        m1 = metrics1.read(bibcode)
        #m2 = metrics2.read('1988PASP..100.1134B')
        m2 = metrics2.read(bibcode)
        if m1 == None or m2 == None:
            return ['BibcodeNotFound:' + bibcode]
        mismatches = []
        fields = ('refereed', 'rn_citations', 'rn_citation_data', 'downloads',
                  'reads', 'an_citations', 'refereed_citation_num', 'citation_num',
                  'reference_num', 'citations', 'refereed_citations', 'author_num', 
                  'an_refereed_citations')
        for field in fields:
            if Metrics.field_mismatch(field, m1, m2): 
                print 'mismatch', bibcode, field  # , m1[field], m2[field]
                mismatches.append(field)
        
        if len(mismatches) > 0:
            return mismatches
        return False;

    @staticmethod
    def field_mismatch(fieldname, m1, m2):
        """test function to compare a field in two different metrics dictionaries"""
        # metrics database has sql null for some reads and downloads while new metrics has array of 0 values
        if m2[fieldname] == None and (m1[fieldname] == [] or m1[fieldname] == [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]):
            return False
        
        if type(m1[fieldname]) != type(m2[fieldname]):
            return True

        if m1[fieldname] == None and m2[fieldname] == None:
            return False

        if fieldname in ('downloads', 'reads'):
            # only last value may be different to account for slightly older test data
            t1 = m1[fieldname][:-1]
            t2 = m2[fieldname][:-1]
            for v1,v2 in zip(t1, t2):
                if v1 != v2:
                    print fieldname, v1, v2
                    return True;
            t1 = m1[fieldname][-1]
            t2 = m2[fieldname][-1]
            delta = abs(t1 - t2)
            limit = max(5, v1 * .15)
            if delta >  limit:
                print fieldname, v1, v2
                return True
            return False

        # if value is an array, compare individual elements
        if isinstance(m1[fieldname], list):
            delta = abs(len(m1[fieldname]) - len(m2[fieldname]))
            if delta > 0 and delta < 3:
                print 'warning: list length', len(m1[fieldname]), len(m2[fieldname])
            elif delta >= 3: 
                return True

            warning_count = 0
            if fieldname == 'rn_citation_data':
                m1_bibs = [element['bibcode'] for element in m1[fieldname]]
                m2_bibs = [element['bibcode'] for element in m2[fieldname]]
                for v in m1_bibs:
                    if v not in m2_bibs:
                        warning_count += 1
                        print 'warning: list element missing', v
                for v in m2_bibs:
                    if v not in m1_bibs:
                        warning_count += 1
                        print 'warning:: list element missing', v
            else:
                for v in m1[fieldname]:
                    if v not in m2[fieldname]:
                        warning_count += 1
                        print 'warning: list element missing', v

                for v in m2[fieldname]:
                    if v not in m1[fieldname]:
                        warning_count += 1
                        print 'warning:: list element missing', v

            if warning_count > 3:
                return True

            return False

        # otherwise, see if scalar values match
        return Metrics.value_mismatch(m1[fieldname], m2[fieldname])

    @staticmethod
    def value_mismatch(v1, v2):
        """test function to compare to scalar values from two different metrics dictionaries"""
        if type(v1) != type(v2):
            print 'type mismatch'
            return True

        if v1 == None or v2 == None:
            if v1 == None and v2 == None: return False
            print 'None mismatch'
            return True

        if isinstance(v1,(str, unicode)):
            mismatch = v1 != v2
            if mismatch:
                print 'mismatch'
                print v1
                print v2
            return mismatch

        if isinstance(v1, bool):
            return v1 != v2
        if isinstance(v1, (int, float)):
            delta = abs(v1 - v2)
            limit = 5  # max(abs(v1), abs(v2)) * .15
            if (delta > limit):
                print 'mismatch', v1, v2 
                return True
            else:
                return False
        if isinstance(v1, dict):
            k1 = v1.keys()
            k2 = v2.keys()
            if len(k1) != len(k2): 
                print 'mismatch', v1, v2 
                return True
            for k in k1:
                if v1[k] != v2[k]: 
                    print 'mismatch', k, v1, v2
                    return True
            return False
        raise ValueError('Unexpected data type passed to value_mismatch,type = ' + str(type(v1)))
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='process row view to Metrics')
    parser.add_argument('-deltaSchema', default=None, help='schema for delta table')
    #parser.add_argument('-full', action='store_const', const=False, help='full import')
    parser.add_argument('-fromScratch', action='store_true', default=False, help='assume empty metrics database')
    parser.add_argument('-copyFromProgram', action='store_true', default=False, help='called from postgres, output to stdout')
    parser.add_argument('command', help='metricsCompute|metricsCompare')
    parser.add_argument('-metricsSchema', default='metrics', help='schema for metrics table')
    parser.add_argument('-rowViewSchema', default='ingest', help='schema for column tables')
    parser.add_argument('-b', '--bibcode', help='bibcode or stdin to read bibcodes')
    parser.add_argument('-startOffset', default=1, help='offset into list of bibcodes to process for chunking support')
    parser.add_argument('-endOffset', default=-1, help='when to stop processing list of bibcodes for chunking support')
    
    args = parser.parse_args()
    if args.command == 'metricsCompute' and args.bibcode:
        m = Metrics(args.metricsSchema,from_scratch = args.fromScratch, copy_from_program = args.copyFromProgram)
        m.update_metrics_test(args.bibcode, args.rowViewSchema)

    elif args.command == 'metricsCompute' and args.deltaSchema:
        print 'saving delta'
        m = Metrics(args.metricsSchema, from_scratch = args.fromScratch, copy_from_program = args.copyFromProgram)
        m.update_metrics_delta(args.bibcode, args.rowViewSchema)

    elif args.command == 'metricsCompute':
        m = Metrics(args.metricsSchema, from_scratch = args.fromScratch, copy_from_program = args.copyFromProgram)
        m.update_metrics_all(args.rowViewSchema, start_offset=int(args.startOffset), end_offset=int(args.endOffset))

    elif args.command == 'metricsCompare' and args.bibcode:
        metrics1 = Metrics(args.metricsSchema, METRICS_DATABASE)
        metrics2 = Metrics('', METRICS_DATABASE2)
        if (args.bibcode == 'stdin'):
            while True:
                line = sys.stdin.readline()
                if len(line) == 0: break
                mismatch = Metrics.metrics_mismatch(line.strip(), metrics1, metrics2)
                if mismatch:
                    print line.strip(), ' MISMATCH = ', mismatch
        else:
            mismatch = Metrics.metrics_mismatch(args.bibcode, metrics1, metrics2)
            print 'mismatch = ', mismatch

