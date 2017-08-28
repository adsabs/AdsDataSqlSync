from sqlalchemy import Column, Integer, Float, String, DateTime, Boolean
from sqlalchemy import Table, bindparam, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema
from sqlalchemy import and_
from sqlalchemy.sql import select
from sqlalchemy.dialects.postgresql import ARRAY
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


class Metrics():
    """computes and provides interface for metrics data"""

    def __init__(self, schema_='metrics'):
        self.logger = setup_logging('AdsDataSqlSync', 'INFO')

        self.schema =  schema_
        self.table = models.MetricsTable()
        self.table.schema = self.schema
        self.from_scratch = True

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

    def get_by_bibcodes(self, db_conn, bibcodes):
        """ return a list of metrics datbase objects matching the list of passed bibcodes"""
        Session = sessionmaker()
        sess = Session(bind=db_conn)
        x = sess.execute(select([self.table], self.table.c.bibcode.in_(bibcodes))).fetchall()
        sess.close()
        return x


    def update_metrics_changed(self, db_conn, nonbib_conn, row_view_schema='ingest'):  
        """changed bibcodes are in sql table, for each we update metrics record"""
        Session = sessionmaker(bind=nonbib_conn)
        nonbib_sess = Session()
        nonbib_sess.execute('set search_path to {}'.format(row_view_schema))
        
        sql_sync = nonbib.NonBib(row_view_schema)
        query = nonbib_sess.query(models.NonBibTable)
        results = query.all()
        count = 0
        for delta_row in results:
            row = sql_sync.get_by_bibcode(nonbib_conn, delta_row.bibcode)
            metrics_dict = self.row_view_to_metrics(row, nonbib_conn, row_view_schema)
            self.save(db_conn, metrics_dict)
            if (count % 10000) == 0:
                self.logger.debug('delta count = {}, bibcode = {}'.format(count, delta_row.bibcode))
            count += 1
        self.flush(db_conn)
        # need to close?

    def update_metrics_bibcode(self, bibcode, nonbib):  #, delta_schema='delta'):
        """changed bibcodes are in sql table, for each we update metrics record"""
        row = nonbib.get_row_view(bibcode)
        metrics_dict = self.row_view_to_metrics(row, sql_sync)
        self.save(metrics_dict)


    # paper from 1988: 1988PASP..100.1134B
    # paper with 3 citations 2015MNRAS.447.1618S
    def update_metrics_test(self, bibcode, row_view_schema='ingest'):
        sql_sync = nonbib.NonBib(row_view_schema)
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
        while True:
            query = session.query(models.NonBibTable).filter(
                and_(
                    models.NonBibTable.id >= offset,
                    models.NonBibTable.id < offset + step_size))
            #s = s.where(sql_sync.table.c.id >= offset).where(sql_sync.table.c.id < offset + step_size)
            results = query.all()
            for row_view_current in results:
                metrics_dict = self.row_view_to_metrics(row_view_current, nonbib_conn, row_view_schema)
                self.save(db_conn, metrics_dict)
                count += 1
                if max_rows > 0 and count > max_rows:
                    break
                if count % 1000 == 0:
                    self.logger.debug('metrics.py, metrics count = {}'.format(count))
                if end_offset > 0 and end_offset <= (count + start_offset):
                    # here if we processed the last requested row
                    self.flush(db_conn)
                    end_time = time.time()
                    return

            #if results.rowcount < step_size:
            if len(results) < step_size:
                # here if last read got the last block of data and we're done processing it
                self.flush(db_conn)
                end_time = time.time()
                return;

            offset += step_size

    # normalized citations:
    #  for a list of N papers (the citations?)
    #  a = number of authors for the publication
    #  c = number of citations tha paper received (why not call it references?)
    #  c/a = normalized citations
    #  sum over N papers
    def row_view_to_metrics(self, passed_row_view, nonbib_db_conn, row_view_schema='nonbib'):
        """convert the passed row view into a complete metrics dictionary"""
        # first do easy fields
        bibcode = passed_row_view.bibcode
        m = models.MetricsTable()
        m.bibcode = bibcode
        m.id = passed_row_view.id
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
            q = 'select refereed,array_length(reference,1),bibcode from ' + row_view_schema + '.RowViewM where bibcode in (select unnest(citations) from ' + row_view_schema + '.RowViewM where bibcode=%s);'
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
    def metrics_mismatch(bibcode, metrics1, metrics2):
        """test function to compare metric records from two different databases"""
        m1 = metrics1.read(bibcode)
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
        m = Metrics(args.metricsSchema)
        m.update_metrics_delta(args.bibcode, args.rowViewSchema)

    elif args.command == 'metricsCompute':
        m = Metrics(args.metricsSchema)
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
        if f:
            while True:
                line = f.readline()
                if len(line) == 0:
                    print 'final saving {} records'.format(len(update_buffer))
                    print datetime.now()
                    if len(update_buffer) > 0:
                        metrics2.connection.execute(metrics2.updater_sql, update_buffer)
                    print i, datetime.now()
                    exit()
                Metrics.metrics_update(line.strip(), metrics1, metrics2, update_buffer)
                i += 1
                if len(update_buffer) >= 100:
                    metrics2.connection.execute(metrics2.updater_sql, update_buffer)
                    update_buffer = []
                    print i, datetime.now()

        else:
            Metrics.metrics_update(args.bibcode, metrics1, metrics2)

