
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
from sqlalchemy.schema import CreateSchema, DropSchema
from collections import defaultdict
from datetime import datetime
import time
import sys
import argparse
import os
import logging
import utils

#from settings import(ROW_VIEW_DATABASE, METRICS_DATABASE, METRICS_DATABASE2)

Base = declarative_base()


# part of sql sync

#sql_sync_engine = create_engine('postgresql://postgres@localhost:5432/postgres', echo=False)

#sql_sync_connection = sql_sync_engine.connect()

class SqlSync:

    all_types = ('canonical', 'author', 'refereed', 'simbad', 'grants', 'citation', 'relevance',
                  'reader', 'download', 'reference', 'reads')

    def __init__(self, schema_name, passed_config=None):
        self.schema_name = schema_name
        self.config = {}
        self.config.update(utils.load_config())
        if passed_config:
            self.config.update(passed_config)
        
        connection_string = self.config.get('INGEST_DATABASE',
                                            'postgresql://postgres@localhost:5432/postgres')
        self.sql_sync_engine = create_engine(connection_string, echo=False)
        self.sql_sync_connection = self.sql_sync_engine.connect()
        self.meta = MetaData()
        self.row_view_table = self.get_row_view_table()
        
        self.logger = logging.getLogger('AdsDataSqlSync')


    def create_column_tables(self):
        self.sql_sync_engine.execute(CreateSchema(self.schema_name))
        temp_meta = MetaData()
        for t in SqlSync.all_types:
            table = self.get_table(t, temp_meta)
        temp_meta.create_all(self.sql_sync_engine)
        self.logger.info('row_view, created database tables for column files')
        

    def drop_column_tables(self):
        self.sql_sync_engine.execute("drop schema if exists {} cascade".format(self.schema_name))
        #temp_meta = MetaData()
        #for t in SqlSync.all_types:
        #    table = self.get_table(t, temp_meta)
        #temp_meta.drop_all(self.sql_sync_engine)
        #self.sql_sync_engine.execute(DropSchema(self.schema_name))
        self.logger.info('row_view, dropped database tables for column files')

    def create_joined_rows(self):
        Session = sessionmaker()
        sess = Session(bind=self.sql_sync_connection)
        sql_command = SqlSync.create_view_sql.format(self.schema_name)
        sess.execute(sql_command)
        sess.commit()
        
        sql_command = 'create index on {}.RowViewM (bibcode)'.format(self.schema_name)
        sess.execute(sql_command)
        sql_command = 'create index on {}.RowViewM (id)'.format(self.schema_name)
        sess.execute(sql_command)
        sess.commit()
        sess.close()
    
    def create_delta_rows(self, baseline_schema):
        Session = sessionmaker()
        sess = Session(bind=self.sql_sync_connection)
        sql_command = SqlSync.create_changed_sql.format(self.schema_name, baseline_schema)
        sess.execute(sql_command)
        sess.commit()
        sess.close()
        
        
    def get_delta_table(self, meta=None):
        """ delta table holds list of bibcodes that differ between two row views"""
        if meta is None:
            meta = self.meta
        return Table('changedrowsm', meta,
                     Column('bibcode', String, primary_key=True),
                     schema=self.schema_name) 

    def get_canonical_table(self, meta=None):
        if meta is None:
            meta = self.meta
        return Table('canonical', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('id', Integer),
                     schema=self.schema_name) 

    def get_author_table(self, meta=None):
        if meta is None:
            meta = self.meta
        return Table('author', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('authors', ARRAY(String)),
                     extend_existing=True,
                     schema=self.schema_name) 

    def get_refereed_table(self, meta=None):
        if meta is None:
            meta = self.meta
        return Table('refereed', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('refereed', Boolean),
                     schema=self.schema_name)

    def get_simbad_table(self, meta=None):
        if meta is None:
            meta = self.meta
        return Table('simbad', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('simbad_objects', ARRAY(String)),
                     schema=self.schema_name) 

    def get_grants_table(self, meta=None):
        if meta is None:
            meta = self.meta
        return Table('grants', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('grants', ARRAY(String)),
                     schema=self.schema_name) 

    def get_citation_table(self, meta=None):
        if meta is None:
            meta = self.meta
        return Table('citation', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('citations', ARRAY(String)),
                     schema=self.schema_name) 

    def get_relevance_table(self, meta=None):
        if meta is None:
            meta = self.meta
        return Table('relevance', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('boost', Float),
                     Column('citation_count', Integer),
                     Column('read_count', Integer),
                     Column('norm_cites', Integer),
                     schema=self.schema_name) 

    def get_reader_table(self, meta=None):
        if meta is None:
            meta = self.meta
        return Table('reader', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('readers', ARRAY(String)),
                     schema=self.schema_name) 

    def get_reads_table(self, meta=None):
        if meta is None:
            meta = self.meta
        return Table('reads', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('reads', ARRAY(Integer)),
                     schema=self.schema_name) 

    def get_download_table(self, meta=None):
        if meta is None:
            meta = self.meta
        return Table('download', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('downloads', ARRAY(Integer)),
                     schema=self.schema_name) 

    def get_reference_table(self, meta=None):
        if meta is None:
            meta = self.meta
        return Table('reference', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('reference', ARRAY(String)),
                     schema=self.schema_name) 


    def get_row_view_table(self, meta=None):
        if meta is None:
            meta = self.meta
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
                     Column('downloads', ARRAY(Integer)),
                     Column('reads', ARRAY(Integer)),
                     Column('reference', ARRAY(String)),
                     schema=self.schema_name,
                     extend_existing=True)

    def get_changed_rows_table(self, table_name, schema_name, meta=None):
        if meta is None:
            meta = self.meta
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

    def get_table(self, table_name, meta=None):
        if meta is None:
            meta = self.meta
        method_name = "get_" + table_name + "_table"
        method = getattr(self, method_name)
        table = method(meta)
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

    create_view_sql =     \
        'CREATE MATERIALIZED VIEW {0}.RowViewM AS  \
         select bibcode,  \
	      id,         \
              coalesce(authors, ARRAY[]::text[]) as authors,    \
              coalesce(refereed, FALSE) as refereed,            \
              coalesce(simbad_objects, ARRAY[]::text[]) as simbad_objects,  \
              coalesce(grants, ARRAY[]::text[]) as grants,      \
              coalesce(citations, ARRAY[]::text[]) as citations,\
              coalesce(boost, 0) as boost,                      \
              coalesce(citation_count, 0) as citation_count,    \
              coalesce(read_count, 0) as read_count,            \
              coalesce(norm_cites, 0) as norm_cites,            \
              coalesce(readers, ARRAY[]::text[]) as readers,    \
              coalesce(downloads, ARRAY[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]) as downloads, \
              coalesce(reference, ARRAY[]::text[]) as reference, \
              coalesce(reads, ARRAY[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]) as reads \
       from {0}.Canonical natural left join {0}.Author \
       natural left join {0}.Refereed                 \
       natural left join {0}.Simbad natural left join {0}.Grants \
       natural left join {0}.Citation                 \
       natural left join {0}.Relevance natural left join {0}.Reader \
       natural left join {0}.Download natural left join {0}.Reads   \
       natural left join {0}.Reference;' 

    create_changed_sql = \
        'create materialized view {0}.ChangedRowsM as \
         select {0}.RowViewM.bibcode, {0}.RowViewM.id \
         from {0}.RowViewM,{1}.RowViewM \
         where {0}.RowViewM.bibcode={1}.RowViewM.bibcode  \
           and ({0}.RowViewM.authors!={1}.RowViewM.authors \
	   or {0}.RowViewM.refereed!={1}.RowViewM.refereed \
	   or {0}.RowViewM.simbad_objects!={1}.RowViewM.simbad_objects \
	   or {0}.RowViewM.grants!={1}.RowViewM.grants \
	   or {0}.RowViewM.citations!={1}.RowViewM.citations \
	   or {0}.RowViewM.boost!={1}.rowViewM.boost \
	   or {0}.RowViewM.norm_cites!={1}.RowViewM.norm_cites \
	   or {0}.RowViewM.citation_count!={1}.RowViewM.citation_count \
	   or {0}.RowViewM.read_count!={1}.RowViewM.read_count \
	   or {0}.RowViewM.readers!={1}.RowViewM.readers \
	   or {0}.RowViewM.downloads!={1}.RowViewM.downloads \
	   or {0}.RowViewM.reads!={1}.RowViewM.reads);'






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

                
        
