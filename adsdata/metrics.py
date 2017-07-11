from sqlalchemy import Column, Integer, Float, String, DateTime, Boolean
from sqlalchemy import Table, bindparam, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import sessionmaker
#from sqlalchemy import create_engine
import sqlalchemy
from sqlalchemy.schema import CreateSchema, DropSchema

from sqlalchemy.sql import select
from sqlalchemy.dialects.postgresql import ARRAY
from collections import defaultdict
from datetime import datetime
import time
import sys
import json
import argparse

from adsputils import load_config, setup_logging
import row_view



#from settings import(ROW_VIEW_DATABASE, METRICS_DATABASE, METRICS_DATABASE2)

Base = declarative_base()
meta = MetaData()

# hack used for batching updates to rds

# part of the Metrics system

class Metrics():
    """computes and provides interface for metrics data"""

    def __init__(self, metrics_schema='metrics', passed_config=None):
        self.config = load_config()
        if passed_config:
            self.config.update(passed_config)
        self.logger = setup_logging('AdsDataSqlSync', level=self.config.get('LOG_LEVEL', 'INFO'))


        self.meta = MetaData()
        self.schema = metrics_schema
        self.table = self.get_metrics_table()
        self.database = self.config.get('METRICS_DATABASE', 'postgresql://postgres:postgres@localhost:5432/postgres')
        self.engine = sqlalchemy.create_engine(self.database, connect_args={'connect_timeout': 3})
        self.connection = self.engine.connect()
        # if true, don't bother checking if metrics database has bibcode, assume db insert
        self.from_scratch = self.config.get('FROM_SCRATCH', True)
        # if true, send data to stdout
        self.copy_from_program = self.config.get('COPY_FROM_PROGRAM', True)
        self.upserts = []

        # sql command to update a row in the metrics table
        self.updater_sql = self.table.update().where(self.table.c.bibcode == bindparam('tmp_bibcode')). \
            values ({'refereed': bindparam('refereed'),
                     'rn_citations': bindparam('rn_citations'),
                     'rn_citation_data': bindparam('rn_citation_data'),
                     'rn_citations_hist': bindparam('rn_citations_hist'),
                     'downloads': bindparam('downloads'),
                     'reads': bindparam('reads'),
                     'an_citations': bindparam('an_citations'),
                     'refereed_citation_num': bindparam('refereed_citation_num'),
                     'citation_num': bindparam('citation_num'),
                     'reference_num': bindparam('reference_num'),
                     'citations': bindparam('citations'),
                     'refereed_citations': bindparam('refereed_citations'),
                     'author_num': bindparam('author_num'),
                     'an_refereed_citations': bindparam('an_refereed_citations')})

        self.tmp_count = 0
        self.tmp_update_buffer = []

    def get_metrics_table(self, meta=None):
        if meta is None:
            meta = self.meta
        return Table('metrics', meta,
                     Column('id', Integer, primary_key=True, autoincrement=True),
                     Column('bibcode', String, nullable=False, index=True, unique=True),
                     Column('refereed',Boolean),
                     Column('rn_citations', postgresql.REAL),
                     Column('rn_citation_data', postgresql.JSON),
                     Column('rn_citations_hist', postgresql.JSON),
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

    def create_metrics_table(self):
        self.engine.execute(CreateSchema(self.schema))
        temp_meta = MetaData()
        table = self.get_metrics_table(temp_meta)
        temp_meta.create_all(self.engine)
        self.logger.info('metrics.py, metrics table created, schema={}'.format(self.schema))


    def drop_metrics_table(self):
        self.engine.execute("drop schema if exists {} cascade".format(self.schema))
        #temp_meta = MetaData()
        #table = self.get_metrics_table(temp_meta)
        #temp_meta.drop_all(self.engine)
        #self.engine.execute(DropSchema(self.schema))
        self.logger.info('metrics.py, metrics table dropped')




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
        if (self.tmp_count % 100) == 0:
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
                    d.pop('id', None)
                    updates.append(d)
                else:
                    d.pop('id', None)
                    inserts.append(d)

        if len(updates):
            result = self.connection.execute(self.updater_sql, updates)
        if len(inserts):
            result = self.connection.execute(self.table.insert(), inserts)

        self.upserts = []


    def read(self, bibcode):
        """read the passed bibcode from the postgres database"""
        s = select([self.table]).where(self.table.c.bibcode == bibcode)
        r = self.connection.execute(s)
        first = r.first()
        return first

    def get_by_bibcodes(self, bibcodes):
        """ return a list of metrics datbase objects matching the list of passed bibcodes"""
        Session = sessionmaker()
        sess = Session(bind=self.connection)
        x = sess.execute(select([self.table], self.table.c.bibcode.in_(bibcodes))).fetchall()
        sess.close()
        return x


    def update_metrics_changed(self, row_view_schema='ingest'):  #, delta_schema='delta'):
        """changed bibcodes are in sql table, for each we update metrics record"""
        self.copy_from_program = False  # maybe hack
        delta_sync = row_view.SqlSync(row_view_schema)
        delta_table = delta_sync.get_delta_table()
        sql_sync = row_view.SqlSync(row_view_schema)
        row_view_table = sql_sync.get_row_view_table()
        connection = sql_sync.engine.connect()
        s = select([delta_table])
        results = connection.execute(s)
        count = 0
        for delta_row in results:
            row = sql_sync.get_row_view(delta_row['bibcode'])
            metrics_dict = self.row_view_to_metrics(row, sql_sync)
            self.save(metrics_dict)
            if (count % 10000) == 0:
                self.logger.debug('delta count = {}, bibcode = {}'.format(count, delta_row['bibcode']))
            count += 1
        self.flush()
        # need to close?

    def update_metrics_bibcode(self, bibcode, sql_sync_connection):  #, delta_schema='delta'):
        """changed bibcodes are in sql table, for each we update metrics record"""
        self.copy_from_program = False  # maybe hack
        row = sql_sync.get_row_view(bibcode)
        metrics_dict = self.row_view_to_metrics(row, sql_sync)
        self.save(metrics_dict)


    # paper from 1988: 1988PASP..100.1134B
    # paper with 3 citations 2015MNRAS.447.1618S
    def update_metrics_test(self, bibcode, row_view_schema='ingest'):
        sql_sync = row_view.SqlSync(row_view_schema)
        row_view_bibcode = sql_sync.get_row_view(bibcode)
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


    def update_metrics_all(self, row_view_schema='ingest', start_offset=1, end_offset=-1):
        """update all elements in the metrics database between the passed id offsets"""
        # we request one block of rows from the database at a time
        start_time = time.time()
        step_size = 1000
        count = 0
        offset = start_offset
        max_rows = self.config['MAX_ROWS']
        sql_sync = row_view.SqlSync(row_view_schema)
        table = sql_sync.get_row_view_table()
        while True:
            s = select([table])
            s = s.where(sql_sync.table.c.id >= offset).where(sql_sync.table.c.id < offset + step_size)
            connection = sql_sync.connection;
            results = connection.execute(s)
            for row_view_current in results:
                metrics_dict = self.row_view_to_metrics(row_view_current, sql_sync)
                self.save(metrics_dict)
                count += 1
                if max_rows > 0 and count > max_rows:
                    break
                if count % 1000 == 0:
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
    def row_view_to_metrics(self, passed_row_view, sql_sync):
        """convert the passed row view into a complete metrics dictionary"""
        # first do easy fields
        bibcode = passed_row_view['bibcode']
        metrics_dict = {'bibcode': bibcode}
        metrics_dict['id'] = passed_row_view['id']
        metrics_dict['refereed'] = passed_row_view['refereed']
        metrics_dict['citations'] = passed_row_view['citations']
        metrics_dict['reads'] = passed_row_view['reads']
        metrics_dict['downloads'] = passed_row_view['downloads']

        metrics_dict['citation_num'] = len(passed_row_view['citations']) if passed_row_view['citations'] else 0
        metrics_dict['author_num'] = max(len(passed_row_view['authors']),1) if passed_row_view['authors'] else 1
        metrics_dict['reference_num'] = len(passed_row_view['reference']) if passed_row_view['reference'] else 0

        #metrics_dict['citation_num'] = len(passed_row_view.get('citations', [])
        #metrics_dict['author_num'] = max(len(passed_row_view.get('authors'),[]),1)
        #metrics_dict['reference_num'] = len(passed_row_view.get('reference'),[])

        # next deal with papers that cite the current one
        # compute histogram, normalized values of citations
        #  and create list of refereed citations
        citations = passed_row_view['citations']
        normalized_reference = 0.0
        citations_json_records = []
        refereed_citations = []
        citations_histogram = defaultdict(float)
        total_normalized_citations = 0.0
        if citations:
            q = 'select refereed,array_length(reference,1),bibcode from ' + sql_sync.schema + '.RowViewM where bibcode in (select unnest(citations) from ' + sql_sync.schema + '.RowViewM where bibcode=%s);'
            result = sql_sync.connection.execute(q,bibcode)
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
                            "auth_norm": 1.0 / metrics_dict['author_num'],
                            "pubyear": int(bibcode[:4]),
                            "cityear": int(citation_bibcode[:4])}
                citations_json_records.append(tmp_json)
                if (citation_refereed):
                    refereed_citations.append(citation_bibcode)
                citations_histogram[citation_bibcode[:4]] += total_normalized_citations

        metrics_dict['refereed_citations'] = refereed_citations
        metrics_dict['refereed_citation_num'] = len(refereed_citations)

        # annual citations
        today = datetime.today()
        resource_age = max(1.0, today.year - int(bibcode[:4]) + 1)
        metrics_dict['an_citations'] = float(metrics_dict['citation_num']) / float(resource_age)
        metrics_dict['an_refereed_citations'] = float(metrics_dict['refereed_citation_num']) / float(resource_age)

        # normalized info
        metrics_dict['rn_citations'] = normalized_reference     # total_normalized_citations
        metrics_dict['rn_citation_data'] = citations_json_records
        metrics_dict['rn_citations_hist'] = dict(citations_histogram)
        metrics_dict['modtime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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
                  'an_refereed_citations', 'rn_citations_hist')
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

    @staticmethod
    def metrics_update(bibcode, metrics_local, metrics_rds, updatebuffer):
        start = datetime.now()
        rds = metrics_rds.read(bibcode)
        local = metrics_local.read(bibcode)
        #fields = ('id', 'refereed', 'rn_citations', 'rn_citation_data', 'rn_citations_hist', 'downloads',
        #          'reads', 'an_citations', 'refereed_citation_num', 'citation_num',
        #          'reference_num', 'citations', 'refereed_citations', 'author_num',
        #          'an_refereed_citations', 'modtime')
        #for field in fields:
        #    print field, rds[field], local[field]

        save_dict = {'bibcode': bibcode}
        local_fields = ('refereed', 'rn_citations', 'rn_citation_data', 'rn_citations_hist', 'downloads',
                        'reads', 'an_citations', 'refereed_citation_num', 'citation_num',
                        'reference_num', 'citations', 'refereed_citations', 'author_num',
                        'an_refereed_citations', 'modtime')
        if local is None:
            return

        for field in local_fields:
            save_dict[field] = local[field]

        if rds:
            # here if there's an existing record in the rds database we want to update it
            # we want to preserve the id field and update the others
            save_dict['id'] = rds['id']
            save_dict['tmp_bibcode'] = bibcode
            # metrics_rds.connection.execute(metrics_rds.updater_sql, [save_dict])
            update_buffer.append(save_dict)
        else:
            # here with a bibcode not in rds
            # do sql insert
            save_dict.pop('id', None)
            print 'performing rds insert'
            metrics_rds.connection.execute(metrics_rds.table.insert(), [save_dict])
        end = datetime.now()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='process row view to Metrics')
    parser.add_argument('-deltaSchema', default=None, help='schema for delta table')
    #parser.add_argument('-full', action='store_const', const=False, help='full import')
    parser.add_argument('-fromScratch', action='store_true', default=False, help='assume empty metrics database')
    parser.add_argument('-copyFromProgram', action='store_true', default=False, help='called from postgres, output to stdout')
    parser.add_argument('command', help='metricsCompute|metricsCompare|metricsUpdate')
    parser.add_argument('-metricsSchema', default='metrics', help='schema for metrics table')
    parser.add_argument('-r', '--rowViewSchema', default='ingest', help='schema for column tables')
    parser.add_argument('-b', '--bibcode', help='bibcode or stdin to read bibcodes')
    parser.add_argument('-f', '--file', help='file of bibcodes, one per line')
    parser.add_argument('-startOffset', default=1, help='offset into list of bibcodes to process for chunking support')
    parser.add_argument('-endOffset', default=-1, help='when to stop processing list of bibcodes for chunking support')

    args = parser.parse_args()
    m = Metrics(args.metricsSchema, {'FROM_SCRATCH': False})
    sql_sync = row_view.SqlSync(args.rowViewSchema)
    if args.command == 'metricsCompute' and args.bibcode:
        if (args.bibcode != 'stdin'):
            m.update_metrics_bibcode(args.bibcode, sql_sync)
            m.flush()
        else:
            count = 0
            while True:
                bibcode = sys.stdin.readline()
                bibcode = bibcode.strip()
                if len(bibcode) == 0:
                    print 'final flush'
                    m.flush()
                    break
                m.update_metrics_bibcode(bibcode, sql_sync)
                count += 1
                if count % 100 == 0:
                    print 'flushing records'
                    m.flush()

    elif args.command == 'metricsCompute' and args.deltaSchema:
        print 'saving delta'
        m = Metrics(args.metricsSchema, from_scratch = args.fromScratch, copy_from_program = args.copyFromProgram)
        m.update_metrics_delta(args.bibcode, args.rowViewSchema)

    elif args.command == 'metricsCompute':
        m = Metrics(args.metricsSchema, from_scratch = args.fromScratch, copy_from_program = args.copyFromProgram)
        m.update_metrics_all(args.rowViewSchema, start_offset=int(args.startOffset), end_offset=int(args.endOffset))

    elif args.command == 'metricsCompare' and args.bibcode:
        metrics1 =  Metrics(args.metricsSchema, {'METRICS_DATABASE': 'postgresql://postgres@localhost:5432/postgres'})
        metrics2 = Metrics('metricsstaging', {'METRICS_DATABASE': 'postgresql://postgres@localhost:5432/postgres'})
        #metrics2 = Metrics('', {'METRICS_DATABASE': 'postgresql://metrics:metrics@adsabs-psql.ci1iae2ep00k.us-east-1.rds.amazonaws.com:5432/metrics'})
        print 'metrics1 {}, metrics2 {}'.format(metrics1, metrics2)
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

    elif args.command == 'metricsUpdate' and args.file:
        metrics1 =  Metrics(args.metricsSchema, {'METRICS_DATABASE': 'postgresql://postgres@localhost:5432/postgres'})
        metrics2 = Metrics('', {'METRICS_DATABASE': 'postgresql://metrics:metrics@testing.ci1iae2ep00k.us-east-1.rds.amazonaws.com:5432/metrics'})
        update_buffer = []
        i = 0
        print 'metrics1 {}, metrics2 {}'.format(metrics1, metrics2)
        f = open(args.file)
        #if (args.bibcode == 'stdin'):
        if f:
            while True:
                line = f.readline()
                #line = sys.stdin.readline()
                #if len(line) == 0: break
                if len(line) == 0:
                    print 'final saving {} records'.format(len(update_buffer))
                    print datetime.now()
                    if len(update_buffer) > 0:
                        metrics2.connection.execute(metrics2.updater_sql, update_buffer)
                    print i, datetime.now()
                    exit()
                    #metrics1.connection.close()
                    #metrics2.connection.close()
                Metrics.metrics_update(line.strip(), metrics1, metrics2, update_buffer)
                i += 1
                if len(update_buffer) >= 100:
                    metrics2.connection.execute(metrics2.updater_sql, update_buffer)
                    update_buffer = []
                    print i, datetime.now()

        else:
            Metrics.metrics_update(args.bibcode, metrics1, metrics2)

