
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

    def get_readers_table(self):
        return Table('readers', meta,
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


    def get_row_view(self, bibcode):
        # connection = sql_sync_engine.connect()
        row_view_select = select([self.row_view_table]).where(self.row_view_table.c.bibcode == bibcode)
        row_view_result = self.sql_sync_connection.execute(row_view_select)
        first = row_view_result.first()
        return first

    def verify(self, data_dir):
        """verify that the data was properly read in"""
        bibcodes_file_size = os.path.getsize(data_dir + '/bibcodes.list.can')
        num_bibcodes_file = bibcodes_file_size / 20  #size of bibcode plus newline
        Session = sessionmaker()
        sess = Session(bind=self.sql_sync_connection)
        num_bibcodes_sql = sess.query(self.row_view_table).count()
        if num_bibcodes_file == num_bibcodes_sql:
            print 'success: database has expected number of bibcodes:', num_bibcodes_file
            return True
        else:
            error = 'error: number of bibcodes do not match: {} in file and {} in database'.format(num_bibcodes_file, num_bibcodes_sql)
            print error
            return False




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
        
