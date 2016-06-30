from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound

from sqlalchemy import *
from sqlalchemy.sql import select
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import sessionmaker
from collections import defaultdict
from datetime import datetime
import time
import sys
import argparse

import row_view

from settings import(ROW_VIEW_DATABASE, METRICS_DATABASE, METRICS_DATABASE2)

Base = declarative_base()
meta = MetaData()

# part of the Metrics system

##metrics_engine = create_engine('postgresql://postgres@localhost:5432/metrics', echo=False)
#metrics_engine = create_engine('postgresql://postgres@localhost:5432/postgres', echo=False)
#metrics_connection = metrics_engine.connect()

class Metrics():

    def __init__(self, metrics_schema='metrics', metrics_database='postgresql://postgres@localhost:5432/postgres'):
        self.table = self.get_metric_table(metrics_schema)
        self.schema = metrics_schema
        self.database = metrics_database
        self.engine = create_engine(self.database)
        self.connection = self.engine.connect()
        self.updates = []
        self.inserts = []
        self.upserts = []
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

    def get_metric_table(self, schema_name):
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
                     schema=schema_name,
                     extend_existing=True)  # do I really want to extend?

    def test_query(self, bibcode):
        q ='select refereed,array_length(reference,1),bibcode from ingest.RowViewM where bibcode = ANY ((select citations from ingest.RowViewM where bibcode=\'' + bibcode + '\')::text[]);'
        result = sql_sync_engine.execute(q)
        for row in result:
            refereed_flag = row[0]
            len_reference = row[1]
            citation_bibcode = row[2]
            print refereed_flag, len_reference, citation_bibcode, '!!'


    def save(self, values_dict):

        bibcode = values_dict['bibcode']
        if bibcode is None:
            print 'error: cannont save metrics data that does not have a bibcode'
            return

        self.upserts.append(values_dict)

        self.tmp_count += 1
        if (self.tmp_count % 100) != 0:
            self.tmp_update_buffer.append(values_dict)
            return

        self.flush()

    def flush(self):

        # first, get list of bibcodes to determine insert/update
        #bibcodes = [d['bibcode'] for d in self.upserts]
        #bibcodes_param = ""
        #for b in bibcodes:
        #    if len(bibcodes_param) == 0:
        #        bibcodes_param = '\'' + b + '\''
        #    else:
        #        bibcodes_param += ',' + '\'' + b + '\''
        #bibcodes_param = '(' + bibcodes_param + ')'
        #q = 'select bibcode from metrics where bibcode in ' + bibcodes_param
        #result = metrics_connection.execute(q)
        #rows = result.fetchall()
        
        #existing_bibcodes = []
        #for row in rows:
        #    existing_bibcodes.append(row[0])
        #print 'number of existing bibcodes = ', len(existing_bibcodes)

        updates = []
        inserts = []
        for d in self.upserts:
            current_bibcode = d['bibcode']
            s = select([self.table]).where(self.table.c.bibcode== current_bibcode)
            r = self.connection.execute(s)
            first = r.first()
            #print 'first = ', first, current_bibcode
            #q = 'select bibcode from metrics where bibcode=%s'
            #result = metrics_connection.execute(q, current_bibcode)
            #first = result.first()
            if first:
                d['tmp_bibcode'] = d['bibcode']
                updates.append(d)
            else:
                inserts.append(d)

        if len(updates):
            result = self.connection.execute(self.u, updates)
            #self.tmp_update_buffer = []
            #self.tmp_count = 0;
            #updater = update(self.table).where(self.table.c.bibcode == bibcode)
            #updater = updater.values(values_dict)
            #result = metrics_connection.execute([updater])
        if len(inserts):
            result = self.connection.execute(self.table.insert(), inserts)
            #print 'inserts incomplete!', len(self.inserts)
            #inserter = self.table.insert(values_dict)
            #result = metrics_connection.execute(inserter)
        self.inserts = []
        self.updates = []
        self.upserts = []
        self.tmp_count = 0
        
    def read(self, bibcode):
        s = select([self.table]).where(self.table.c.bibcode == bibcode)
        r = self.connection.execute(s)
        first = r.first()
        return first

    def update_metrics_changed(self, row_view_schema='ingest'):
        sql_sync = RowView(row_view_schema)
        row_view = sql_sync.get_changed_rows_table('changedrowsm', 'public')
        connection = sql_sync_engine.connect()
        s = select(['row_view'])
        results = connection.execute(s)
        for row_view in results:
            metrics_dict = self.row_view_to_metrics(row_view)
            self.save(metrics_dict)
            self.flush()

    # paper from 1988: 1988PASP..100.1134B
    # paper with 3 citations 2015MNRAS.447.1618S
    def update_metrics_test(self, bibcode, row_view_schema='ingest'):
        sql_sync = RowView(row_view_schema)
        row_view = sql_sync.read_row_view(bibcode)
        print row_view
        print row_view['bibcode']
        metrics_dict = self.row_view_to_metrics(row_view, sql_sync)
        self.save(metrics_dict)
        self.flush()

    def update_metrics_all(self, row_view_schema='ingest'):
        start_time = time.time()
        step_size = 1000
        count = 0
        offset =  0
        max_count = 11500000
        sql_sync = RowView(row_view_schema)
        table = sql_sync.get_row_view_table()
        while count < max_count:
            s = select([table])
            s = s.limit(step_size).offset(offset).order_by('bibcode')
            print 'getting data...', count, offset
            connection = sql_sync.sql_sync_engine.connect()
            results = connection.execute(s)
            print 'received data'
            for row_view in results:
                metrics_dict = self.row_view_to_metrics(row_view, sql_sync)
                self.save(metrics_dict)
                if count % 1000 == 0:
                    print row_view.bibcode
                count += 1

            offset += step_size
        end_time = time.time()
        print 'done:', end_time - start_time

    # normalized citations:
    #  for a list of N papers (the citations?)
    #  a = number of authors for the publication 
    #  c = number of citations tha paper received (why not call it references?)
    #  c/a = normalized citations
    #  sum over N papers
    def row_view_to_metrics(self, row_view, sql_sync):
        bibcode = row_view['bibcode']
        metrics_dict = {'bibcode': bibcode}
        metrics_dict['refereed'] = row_view['refereed']
        metrics_dict['citations'] = row_view['citations']
        metrics_dict['reads'] = row_view['reads']
        metrics_dict['downloads'] = row_view['downloads']
        
        metrics_dict['citation_num'] = len(row_view['citations']) if row_view['citations'] else 0
        metrics_dict['author_num'] = max(len(row_view['authors']),1) if row_view['authors'] else 1
        metrics_dict['reference_num'] = len(row_view['reference']) if row_view['reference'] else 0


        # compute histogram, normalized values of citations
        #  and create list of refereed citations
        citations = row_view['citations']
        normalized_reference = 0.0
        # citations_histogram = defaultdict(float)
        citations_json_records = []
        refereed_citations = []
        total_normalized_citations = 0.0
        if citations:
            #q = 'select refereed,array_length(reference,1),bibcode from ingest.RowViewM where bibcode = ANY ((select citations from ingest.RowViewM where bibcode=%s)::text[]);'
            # the following is another option, but speed is roughly the same
            q = 'select refereed,array_length(reference,1),bibcode from ingest.RowViewM where bibcode in (select unnest(citations) from ingest.RowViewM where bibcode=%s);'

            result = sql_sync.sql_sync_connection.execute(q,bibcode)
            for row in result:
                citation_refereed = row[0] if row[0] else False
                citation_refereed = citation_refereed in (True, 't', 'true')
                len_citation_reference = int(row[1]) if row[1] else 0
                citation_bibcode = row[2]

                citation_normalized_references = 1.0 / float(max(5, len_citation_reference))
                normalized_reference += citation_normalized_references
                citations_json_records.append({'bibcode': citation_bibcode, 'ref_norm': citation_normalized_references, 'auth_norm': 1.0 / metrics_dict['author_num'], 
                                               'pubyear': int(bibcode[:4]), 'cityear': int(citation_bibcode[:4])})

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
                print field, m1[field], m2[field]
                mismatches.append(field)
        
        if len(mismatches) > 0:
            return mismatches
        return False;

    @staticmethod
    def field_mismatch(fieldname, m1, m2):
        
        # hack because we have bad defaults in new sql
        # match None to False and []
        if (m1[fieldname] == None):
            if m2[fieldname] == False or m2[fieldname] == []:
                return False  
                                               
        
        if type(m1[fieldname]) != type(m2[fieldname]):
            return True

        if m1[fieldname] == None and m2[fieldname] == None:
            return False

        if fieldname in ('downloads', 'reads'):
            for v1,v2 in zip(m1[fieldname], m2[fieldname]):
                limit = max(3, v1 * .1)
                if abs(v1 - v2) > limit:
                    print fieldname, v1, v2
                    return True;
            return False

        # if value is an array, compare individual elements
        if isinstance(m1[fieldname], list):
            if len(m1[fieldname]) != len(m2[fieldname]):
                print 'list length', len(m1[fieldname]), len(m2[fieldname])
                return True
            for v in m1[fieldname]:
                if v not in m2[fieldname]:
                    print 'list element missing', v
                    return True
            #for v1,v2 in zip(m1[fieldname],m2[fieldname]):
            #    mismatch = Metrics.value_mismatch(v1,v2)
            #    if mismatch:
            #        print 'list mismatch', v1, v2
            #        return True
            return False

        # otherwise, see if scalar values match
        return Metrics.value_mismatch(m1[fieldname], m2[fieldname])

    @staticmethod
    def value_mismatch(v1, v2):
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
    parser = argparse.ArgumentParser(description='process column files')
    parser.add_argument('-delta', action='store_const', const=True, help='only delta')
    parser.add_argument('-full', action='store_const', const=False, help='full import')
    parser.add_argument('command', help='metricsCompute|metricsCompare')
    parser.add_argument('-metricsSchema', default='metrics', help='schema for metrics table')
    parser.add_argument('-rowViewSchema', default='ingest', help='schema for column tables')
    parser.add_argument('-b', '--bibcode', help='bibcode or stdin to read bibcodes')
    
    args = parser.parse_args()
    print 'processing: ', args
    if args.command == 'metricsCompute' and args.bibcode:
        m = Metrics(args.metricsSchema)
        m.update_metrics_test(args.bibcode, args.rowViewSchema)
        print 'metrics bibcode computed for', args.bibcode
    elif args.command == 'metricsCompute' and args.delta:
        print 'saving delta not imlemented'
    elif args.command == 'metricsCompute':
        m = Metrics(args.metricsSchema)
        m.update_metrics_all()
    elif args.command == 'metricsCompare' and args.bibcode:
        metrics1 = Metrics(args.metricsSchema, METRICS_DATABASE)
        metrics2 = Metrics(args.metricsSchema, METRICS_DATABASE2)
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

