
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
import os

#from settings import(ROW_VIEW_DATABASE, METRICS_DATABASE, METRICS_DATABASE2)

Base = declarative_base()


# part of sql sync

#sql_sync_engine = create_engine('postgresql://postgres@localhost:5432/postgres', echo=False)
meta = MetaData()
#sql_sync_connection = sql_sync_engine.connect()

class SqlSync:

    def __init__(self, schema_name):
        self.schema_name = schema_name
        
        self.sql_sync_engine = create_engine('postgresql://postgres@localhost:5432/postgres', echo=False)
        self.sql_sync_connection = self.sql_sync_engine.connect()
        self.row_view_table = self.get_row_view_table()

    def get_delta_table(self):
        return Table('ChangedRows', meta,
                     Column('bibcode', String, primary_key=True),
                     schema=self.schema_name) 

    def get_canonical_table(self):
        return Table('canonical', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('id', Integer),
                     schema=self.schema_name) 

    def get_author_table(self):
        return Table('author', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('authors', ARRAY(String)),
                     extend_existing=True,
                     schema=self.schema_name) 

    def get_refereed_table(self):
        return Table('refereed', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('refereed', Boolean),
                     schema=self.schema_name)

    def get_simbad_table(self):
        return Table('simbad', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('simbad_objects', ARRAY(String)),
                     schema=self.schema_name) 

    def get_grants_table(self):
        return Table('grants', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('grants', ARRAY(String)),
                     schema=self.schema_name) 

    def get_citation_table(self):
        return Table('citation', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('citations', ARRAY(String)),
                     schema=self.schema_name) 

    def get_relevance_table(self):
        return Table('relevance', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('boost', Float),
                     Column('citation_count', Integer),
                     Column('read_count', Integer),
                     Column('norm_cites', Integer),
                     schema=self.schema_name) 

    def get_reader_table(self):
        return Table('reader', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('readers', ARRAY(String)),
                     schema=self.schema_name) 

    def get_reads_table(self):
        return Table('reads', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('reads', ARRAY(String)),
                     schema=self.schema_name) 

    def get_download_table(self):
        return Table('download', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('downloads', ARRAY(String)),
                     schema=self.schema_name) 

    def get_reference_table(self):
        return Table('reference', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('reference', ARRAY(String)),
                     schema=self.schema_name) 


    def get_row_view_table(self):
        return Table('rowviewm', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('id', Integer),
                     Column('authors', ARRAY(String)),
                     Column('refereed', Boolean),
                     Column('simbad_objects', ARRAY(String)),
                     Column('grants', ARRAY(String)),
                     Column('citations', ARRAY(String)),
                     Column('boost', Float),
                     Column('citation_count', Integer),
                     Column('read_count', Integer),
                     Column('norm_cites', Integer),
                     Column('readers', ARRAY(String)),
                     Column('downloads', ARRAY(String)),
                     Column('reads', ARRAY(String)),
                     Column('reference', ARRAY(String)),
                     schema=self.schema_name,
                     extend_existing=True)

    def get_changed_rows_table(self, table_name, schema_name):
        return Table('table_name', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('id', Integer),
                     Column('authors', ARRAY(String)),
                     Column('refereed', Boolean),
                     Column('simbad_objects', ARRAY(String)),
                     Column('grants', ARRAY(String)),
                     Column('citations', ARRAY(String)),
                     Column('boost', Float),
                     Column('citation_count', Integer),
                     Column('read_count', Integer),
                     Column('norm_cites', Integer),
                     Column('readers', ARRAY(String)),
                     Column('downloads', ARRAY(String)),
                     Column('reads', ARRAY(String)),
                     Column('reference', ARRAY(String)),
                     schema=schema_name,
                     extend_existing=True)

    def get_table(self, table_name):
        method_name = "get_" + table_name + "_table"
        method = getattr(self, method_name)
        table = method()
        return table


    def get_row_view(self, bibcode):
        # connection = sql_sync_engine.connect()
        row_view_select = select([self.row_view_table]).where(self.row_view_table.c.bibcode == bibcode)
        row_view_result = self.sql_sync_connection.execute(row_view_select)
        first = row_view_result.first()
        return first

    def verify(self, data_dir):
        """verify that the data was properly read in
        we only check files that don't repeat bibcodes, so the
        number of bibcodes equals the number of records
        """
        self.verify_aux(data_dir, '/bibcodes.list.can', self.get_canonical_table())
        self.verify_aux(data_dir, '/facet_authors/all.links', self.get_author_table())
        self.verify_aux(data_dir, '/reads/all.links', self.get_reads_table())
        self.verify_aux(data_dir, '/reads/downloads.links', self.get_download_table())
        self.verify_aux(data_dir, '/refereed/all.links', self.get_refereed_table())
        self.verify_aux(data_dir, '/relevance/docmetrics.tab', self.get_relevance_table())


    def verify_aux(self, data_dir, file_name, sql_table):
        Session = sessionmaker()
        sess = Session(bind=self.sql_sync_connection)
        file = data_dir + file_name
        file_count = self.count_lines(file)
        sql_count = sess.query(sql_table).count()
        if file_count != sql_count:
            print file_name, 'count mismatch:', file_count, sql_count
            return False
        return True

        

    def count_lines(self, file):
        count = 0
        with open(file, "r") as f:
            for line in f:
                count += 1
        return count



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='verify ingest of column files')
    parser.add_argument('command', help='verify')
    parser.add_argument('-rowViewSchema', default='ingest', help='schema for column tables')
    parser.add_argument('-dataDir', help='directory for column files', )
    args = parser.parse_args()
    if args.command == 'verify':
        if not args.dataDir:
            print 'argument -dataDir required'
            sys.exit(2)
        row_view = SqlSync(args.rowViewSchema)
        verify = row_view.verify(args.dataDir)
        if verify:
            sys.exit(0)
        sys.exit(1)
    if args.command == 'downloads':
        row_view = SqlSync(args.rowViewSchema)
        Session = sessionmaker()
        sess = Session(bind=row_view.sql_sync_connection)
        t = row_view.get_download_table()
        count = 0
        with open(args.dataDir + '/reads/d1.txt') as f:
            for line in f:
                s = select([t]).where(t.c.bibcode == line.strip())
                rp = row_view.sql_sync_connection.execute(s)
                r = rp.first()
                if r is None:
                    print 'error', line, len(line)
                count += 1
                if count % 1000000 == 0:
                    print count
        print 'count = ', count

                
        
