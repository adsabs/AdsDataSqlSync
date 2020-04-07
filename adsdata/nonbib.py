
from sqlalchemy import Column, Integer, Float, String, DateTime, Boolean
from sqlalchemy import Table, bindparam, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import sessionmaker, load_only
from sqlalchemy import create_engine
from sqlalchemy.sql import select
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.schema import CreateSchema, DropSchema
import sys
import argparse

from adsputils import load_config, setup_logging
import models

Base = declarative_base()


class NonBib:
    """manages 12 fields of nonbibliographic data

    Each nonbib data file is ingested from flat/column files to simple tables
    using Postgres' efficient COPY tablename FROM PROGRAM.
    These tables are joined to create a unified row view of nonbib data.

    We use Postgres schemas so we can use sql to compare one set of ingested data against another.
    """

    all_types = ('canonical', 'author', 'refereed', 'simbad', 'grants', 'citation', 'relevance',
                 'reader', 'download', 'reference', 'reads', 'ned', 'pub_openaccess',
                 'private', 'ocrabstract', 'nonarticle',
                 'datalinks')

    def __init__(self, schema_='nonbib'):
        self.schema = schema_
        self.meta = MetaData()
        self.table = models.NonBibTable()
        self.table.schema = self.schema
        self.logger = setup_logging('AdsDataSqlSync', 'INFO')


    def create_column_tables(self, db_engine):
        """create tables to read nonbib files into

        note that this function does not create the joined row"""
        db_engine.execute(CreateSchema(self.schema))
        for t in models.column_tables:
            table = t()
            table.__table__.schema = self.schema
            table.__table__.create(db_engine)
            sql = 'ALTER TABLE {}.{} SET UNLOGGED;'.format(self.schema, t.__tablename__)
            db_engine.execute(sql)

        self.logger.info('row_view, created database column tables in schema {}'.format(self.schema))


    def rename_schema(self, db_engine, new_name):
        db_engine.execute("alter schema {} rename to {}".format(self.schema, new_name))
        self.logger.info('row_view, renamed schema {} to {} '.format(self.schema, new_name))

    def drop_column_tables(self, db_engine):
        """drop the entire schema, including joined view and delta"""
        db_engine.execute("drop schema if exists {} cascade".format(self.schema))
        #temp_meta = MetaData()
        #for t in SqlSync.all_types:
        #    table = self.get_table(t, temp_meta)
        #temp_meta.drop_all(self.engine)
        #db_engine.execute(DropSchema(self.schema))
        self.logger.info('row_view, dropped database column tables in schema {}'.format(self.schema))

    def create_joined_rows(self, db_conn):
        """join sql tables initialized from the flat/column files into a unified row view"""
        self.logger.info('row_view, creating joined materialized view in schema {}'.format(self.schema))
        Session = sessionmaker()
        sess = Session(bind=db_conn)
        sql_command = NonBib.create_view_sql.format(self.schema)
        sess.execute(sql_command)
        sess.commit()

        sql_command = 'create index on {}.RowViewM (bibcode)'.format(self.schema)
        sess.execute(sql_command)
        sql_command = 'create index on {}.RowViewM (id)'.format(self.schema)
        sess.execute(sql_command)
        sess.commit()
        sess.close()
        self.logger.info('row_view, joined rows in schema {}'.format(self.schema))

    def create_delta_rows(self, db_conn, baseline_schema):
        self.logger.info('row_view, creating delta/changed and new table in schema {}'.format(self.schema))
        Session = sessionmaker()
        sess = Session(bind=db_conn)
        sql_command = NonBib.create_changed_sql.format(self.schema, baseline_schema)
        sess.execute(sql_command)
        sess.commit()
        sql_command = NonBib.include_new_bibcodes_sql.format(self.schema, baseline_schema)
        sess.execute(sql_command)
        sess.commit()
        # resolver
        sql_command = NonBib.include_updated_resolver_bibcodes_sql.format(self.schema, baseline_schema)
        sess.execute(sql_command)
        sess.commit()
        sess.close()
        self.logger.info('row_view, created delta/changed and new table in schema {}'.format(self.schema))


    def get_delta_table(self, meta=None):
        """ delta table holds list of bibcodes that differ between two row views"""
        if meta is None:
            meta = self.meta
        return Table('changedrowsm', meta,
                     Column('bibcode', String, primary_key=True),
                     schema=self.schema)

    def get_new_bibcodes_table(self, meta=None):
        """ table holds list of bibcodes that are not in baseline"""
        if meta is None:
            meta = self.meta
        return Table('newbibcodes', meta,
                     Column('bibcode', String, primary_key=True),
                     schema=self.schema)

    def build_new_bibcodes(self, db_conn, baseline_schema):
        self.logger.info('row_view, dropping and creating new_bibcodes table in schema {}'.format(self.schema))
        self.engine.execute("drop table  if exists {}.newbibcodes;".format(self.schema))
        temp_meta = MetaData()
        table = self.get_new_bibcodes_table(temp_meta)
        temp_meta.create_all(self.engine)

        self.logger.info('row_view, created new_bibcodes table in schema {}'.format(self.schema))

        self.logger.info('row_view, populating new_bibcodes table in schame {}'.format(self.schema))
        Session = sessionmaker()
        sess = Session(bind=db_conn)
        sql_command = NonBib.populate_new_bibcodes_sql.format(self.schema, baseline_schema)
        sess.execute(sql_command)
        sess.commit()
        # resolver
        sql_command = NonBib.populate_new_resolver_bibcodes_sql.format(self.schema, baseline_schema)
        sess.execute(sql_command)
        sess.commit()
        sess.close()
        self.logger.info('row_view, populated new_bibcodes table in schame {}'.format(self.schema))





    def log_delta_reasons(self, db_conn, baseline_schema):
        """log the counts for the changes in each column from baseline """
        Session = sessionmaker()
        sess = Session(bind=db_conn)
        sql_command = 'select count(*) from ' + self.schema + '.changedrowsm'
        r = sess.execute(sql_command)
        m = 'nonbib delta, total number of changed bibcodes: {}'.format(r.scalar())
        print m
        self.logger.info(m)

        column_names = ('authors', 'refereed', 'simbad_objects', 'grants', 'citations',
                        'boost', 'citation_count', 'read_count', 'norm_cites',
                        'readers', 'downloads', 'reads', 'reference', 'ned_objects')
        for column_name in column_names:
            if column_name != 'canonical':
                sql_command = 'select count(*) from ' + self.schema \
                    + '.rowviewm, ' + baseline_schema + '.rowviewm ' \
                    + ' where ' + self.schema + '.rowviewm.bibcode=' + baseline_schema + '.rowviewm.bibcode' \
                    + ' and ' + self.schema + '.rowviewm.' + column_name + '!=' + baseline_schema + '.rowviewm.' + column_name+ ';'

            r = sess.execute(sql_command)
            m = 'nonbib delta, number of {} different: {}'.format(column_name, r.scalar())
            print m
            self.logger.info(m)
        sess.commit()
        sess.close()



    def get_changed_rows_table(self, table_name, schema_name, meta=None):
        if meta is None:
            meta = self.meta
        return Table('table_name', meta,
                     Column('bibcode', String, primary_key=True),
                     Column('id', Integer),
                     Column('authors', ARRAY(String)),
                     Column('refereed', Boolean),
                     Column('pub_openaccess', Boolean),
                     Column('private', Boolean),
                     Column('nonarticle', Boolean),
                     Column('ocrabstract', Boolean),
                     Column('simbad_objects', ARRAY(String)),
                     Column('ned_objects', ARRAY(String)),
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

#    def get_table(self, table_name, meta=None):
#        if meta is None:
#            meta = self.meta
#        method_name = "get_" + table_name + "_table"
#        method = getattr(self, method_name)
#        table = method(meta)
#        return table


    def get_by_bibcode(self, db_conn, bibcode, load_columns=None):
        """read unified nonbib data row for bibcode

        last argument is passed to sqlalchemy load_only
        """
        Session = sessionmaker(bind=db_conn)
        session = Session()
        models.NonBibTable.__table__.schema = self.schema
        q = session.query(models.NonBibTable).filter(models.NonBibTable.bibcode==bibcode)
        if load_columns:
            q.options(load_only(*load_columns))
        first = q.first()
        session.close()
        return first

    def read(self, db_conn, bibcode):
        """this function is used by utils where a standard name is required"""
        return self.get_by_bibcode(db_conn, bibcode)

    def get_by_bibcodes(self, db_conn, bibcodes):
        """ return a list of row view datbase objects matching the list of passed bibcodes"""
        Session = sessionmaker()
        sess = Session(bind=db_conn)
        query = sess.query(models.NonBibTable).filter(models.NonBibTable.bibcode.in_(bibcodes))
        results = query.all()
        sess.close()
        return results


    def verify(self, db_conn, data_dir):
        """verify that the data was properly read in
        we only check files that don't repeat bibcodes, so the
        number of bibcodes equals the number of records
        """
        #self.verify_aux(db_conn, data_dir, '/bibcodes.list.can', self.get_canonical_table())
        #self.verify_aux(db_conn, data_dir, '/facet_authors/all.links', self.get_author_table())
        #self.verify_aux(db_conn, data_dir, '/reads/all.links', self.get_reads_table())
        #self.verify_aux(db_conn, data_dir, '/reads/downloads.links', self.get_download_table())
        #self.verify_aux(db_conn, data_dir, '/refereed/all.links', self.get_refereed_table())
        #self.verify_aux(db_conn ,data_dir, '/relevance/docmetrics.tab', self.get_relevance_table())


    def verify_aux(self, db_conn, data_dir, file_name, sql_table):
        Session = sessionmaker()
        sess = Session(bind=db_conn)
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
        'CREATE MATERIALIZED VIEW {0}.rowviewm AS  \
         select bibcode,  \
	      id,         \
              coalesce(authors, ARRAY[]::text[]) as authors,    \
              coalesce(refereed, FALSE) as refereed,            \
              coalesce(pub_openaccess, FALSE) as pub_openaccess,            \
              coalesce(private, FALSE) as private,            \
              coalesce(nonarticle, FALSE) as nonarticle,            \
              coalesce(ocrabstract, FALSE) as ocrabstract,            \
              coalesce(simbad_objects, ARRAY[]::text[]) as simbad_objects,  \
              coalesce(ned_objects, ARRAY[]::text[]) as ned_objects,  \
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
       natural left join {0}.pub_openaccess           \
       natural left join {0}.private           \
       natural left join {0}.nonarticle           \
       natural left join {0}.ocrabstract           \
       natural left join {0}.Simbad natural left join {0}.Grants \
       natural left join {0}.Citation  natural left join {0}.Ned   \
       natural left join {0}.Relevance natural left join {0}.Reader \
       natural left join {0}.Download natural left join {0}.Reads   \
       natural left join {0}.Reference;'

    create_changed_sql = \
        'create table {0}.ChangedRowsM as \
         select {0}.RowViewM.bibcode, {0}.RowViewM.id \
         from {0}.RowViewM,{1}.RowViewM \
         where {0}.RowViewM.bibcode={1}.RowViewM.bibcode  \
           and ({0}.RowViewM.authors!={1}.RowViewM.authors \
	   or {0}.RowViewM.refereed!={1}.RowViewM.refereed \
	   or {0}.RowViewM.pub_openaccess!={1}.RowViewM.pub_openaccess \
	   or {0}.RowViewM.private!={1}.RowViewM.private \
	   or {0}.RowViewM.nonarticle!={1}.RowViewM.nonarticle \
	   or {0}.RowViewM.ocrabstract!={1}.RowViewM.ocrabstract \
	   or {0}.RowViewM.simbad_objects!={1}.RowViewM.simbad_objects \
           or {0}.RowViewM.ned_objects!={1}.RowViewM.ned_objects \
	   or {0}.RowViewM.grants!={1}.RowViewM.grants \
	   or {0}.RowViewM.citations!={1}.RowViewM.citations \
	   or {0}.RowViewM.boost!={1}.rowViewM.boost \
	   or {0}.RowViewM.norm_cites!={1}.RowViewM.norm_cites \
	   or {0}.RowViewM.citation_count!={1}.RowViewM.citation_count \
	   or {0}.RowViewM.read_count!={1}.RowViewM.read_count \
	   or {0}.RowViewM.readers!={1}.RowViewM.readers \
	   or {0}.RowViewM.downloads!={1}.RowViewM.downloads \
	   or {0}.RowViewM.reads!={1}.RowViewM.reads);'

    # add the new bibcods to the table of changed bibcodes
    include_new_bibcodes_sql = \
        'insert into {0}.ChangedRowsM (bibcode) \
            select {0}.canonical.bibcode from {0}.canonical left join {1}.canonical \
            on {0}.canonical.bibcode = {1}.canonical.bibcode \
            where {1}.canonical.bibcode IS NULL;'

    populate_new_bibcodes_sql = \
        'insert into {0}.newbibcodes (bibcode) \
            select {0}.canonical.bibcode from {0}.canonical left join {1}.canonical \
            on {0}.canonical.bibcode = {1}.canonical.bibcode \
            where {1}.canonical.bibcode IS NULL;'

    # resolver
    include_updated_resolver_bibcodes_sql = \
        'insert into {0}.ChangedRowsM (bibcode, id) \
            select distinct on (bibcode) datalinks.bibcode, {0}.canonical.id from \
               (select {0}.datalinks.bibcode from {0}.datalinks left join {1}.datalinks \
                on {0}.datalinks.bibcode = {1}.datalinks.bibcode \
                and {0}.datalinks.link_type = {1}.datalinks.link_type \
                and {0}.datalinks.link_sub_type = {1}.datalinks.link_sub_type \
                where {0}.datalinks.url != {1}.datalinks.url \
                or {0}.datalinks.title != {1}.datalinks.title \
                or {0}.datalinks.item_count != {1}.datalinks.item_count \
                or {1}.datalinks.bibcode IS NULL) as datalinks \
            left join {0}.canonical \
            on datalinks.bibcode = {0}.canonical.bibcode \
            where \
                {0}.canonical.bibcode IS NOT NULL \
                and not exists (select \'x\' from {0}.ChangedRowsM where {0}.ChangedRowsM.bibcode = datalinks.bibcode);'

    populate_new_resolver_bibcodes_sql = \
        'insert into {0}.newbibcodes (bibcode) \
            select distinct on (bibcode) bibcode from \
               (select {0}.datalinks.bibcode from {0}.datalinks left join {1}.datalinks \
                on {0}.datalinks.bibcode = {1}.datalinks.bibcode \
                and {0}.datalinks.link_type = {1}.datalinks.link_type \
                and {0}.datalinks.link_sub_type = {1}.datalinks.link_sub_type \
                where {1}.datalinks.bibcode IS NULL) as datalinks \
            left join {0}.canonical \
            on datalinks.bibcode = {0}.canonical.bibcode \
            where \
                {0}.canonical.bibcode IS NOT NULL \
                and not exists (select \'x\' from {0}.newbibcodes where {0}.newbibcodes.bibcode = datalinks.bibcode);'


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
        row_view = NonBib(args.rowViewSchema)
        verify = row_view.verify(args.dataDir)
        if verify:
            sys.exit(0)
        sys.exit(1)
    if args.command == 'downloads':
        row_view = NonBib(args.rowViewSchema)
        Session = sessionmaker()
        sess = Session(bind=row_view.connection)
        t = row_view.get_download_table()
        count = 0
        with open(args.dataDir + '/reads/d1.txt') as f:
            for line in f:
                s = select([t]).where(t.c.bibcode == line.strip())
                rp = row_view.connection.execute(s)
                r = rp.first()
                if r is None:
                    print 'error', line, len(line)
                count += 1
                if count % 1000000 == 0:
                    print count
        print 'count = ', count



