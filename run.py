
import sys
import re
import argparse
import os
from sqlalchemy.orm import sessionmaker, load_only
from sqlalchemy.sql import select
from sqlalchemy import create_engine

from adsdata import nonbib
from adsdata import metrics
from adsdata import reader
from adsdata import models
from adsputils import load_config, setup_logging
from adsmsg import NonBibRecord, NonBibRecordList, MetricsRecord, MetricsRecordList
from adsdata.tasks import task_output_results, task_output_metrics

logger = None
config = {}

# non-link fields that are sent to master pipeline
nonbib_to_master_fields = ('bibcode', 'boost', 'citation_count',
                           'grants', 'ned_objects', 'read_count', 
                           'readers', 'reference', 'simbad_objects')

def load_column_files(config, nonbib_db_engine, nonbib_db_conn, sql_sync):
    """ use psycopg.copy_from to data from column file to postgres
    
    after data has been loaded, join to create a unified row view 
    """
    raw_conn = nonbib_db_engine.raw_connection()
    cur = raw_conn.cursor()
    
    for t in nonbib.NonBib.all_types:
        table_name = sql_sync.schema + '.' + t
        logger.info('processing {}'.format(table_name))
        # here we have to read multiple files, information in config file are in a list
        if t == 'datalinks':
            load_column_files_datalinks_table(config[t.upper()], table_name, t, raw_conn, cur)
        else:
            filename = config['DATA_PATH'] + config[t.upper()]
            if t == 'canonical':
                r = reader.BibcodeFileReader(filename)
            elif t == 'refereed':
                r = reader.RefereedFileReader(filename)
            else:
                r = reader.StandardFileReader(t, filename)
            if r:
                cur.copy_from(r, table_name)
                raw_conn.commit()

    cur.close()
    raw_conn.close()
    sql_sync.create_joined_rows(nonbib_db_conn)


def load_column_files_datalinks_table(from_config, table_name, file_type, raw_conn, cur):

    # from_config is a list of lines that could have one the following two formats
    # path,link_type,link_sub_type (i.e., config/links/eprint_html/all.links,ARTICLE,EPRINT_HTML) or
    # path,link_type (i.e., config/links/video/all.links,PRESENTATION)
    for oneLinkType in from_config:
        if (oneLinkType.count(',') == 1):
            [filename, linktype] = oneLinkType.split(',')
            linksubtype = 'NA'
        elif (oneLinkType.count(',') == 2):
            [filename, linktype, linksubtype] = oneLinkType.split(',')
        else:
            return

        if linktype == 'ASSOCIATED':
            r = reader.DataLinksWithTitleFileReader(file_type, config['DATA_PATH'] + filename, linktype)
        elif linktype == 'DATA':
            r = reader.DataLinksWithTargetFileReader(file_type, config['DATA_PATH'] + filename, linktype)
        else:
            r = reader.DataLinksFileReader(file_type, config['DATA_PATH'] + filename, linktype, linksubtype)

        if r:
            cur.copy_from(r, table_name)
            raw_conn.commit()


def nonbib_to_master_dict(row):
    """create dict using only nonbib fields sent to master in protobuf"""
    d = {}
    for column in nonbib_to_master_fields:
        d[column] = getattr(row, column)
    return d


def row2dict(row):
    """ from https://stackoverflow.com/questions/1958219/convert-sqlalchemy-row-object-to-python-dict"""
    d = {}
    for column in row.__table__.columns:
        d[column.name] = getattr(row, column.name)
    return d


def fetch_data_link_elements(query_result):
    elements = []
    if (query_result[0] != None):
        for e in query_result[0].split(','):
            if (e != None):
                elements.append(e)
    return elements


def fetch_data_link_elements_counts(query_result):
    elements = []
    cumulative_count = 0
    if (query_result[1] != None):
        for e in query_result[1].split(','):
            if (e != None):
                elements.append(e)
        cumulative_count = query_result[0]
    return [elements, cumulative_count]


def fetch_data_link_record(query_result):
    # since I want to use this function from the test side,
    # I was not able to use the elegant function row2dict function
    columns = ('link_type', 'link_sub_type', 'url', 'title', 'item_count')
    results = []
    for row in query_result:
        fieldList = []
        for i, field in enumerate(row):
            toList = []
            if columns[i] in ('url', 'title'):
                many = ''.join(field).lstrip('{').rstrip('}').replace('"','').split(',')
                for one in many:
                    toList.append(one)
                fieldList.append(toList)
            else:
                fieldList.append(field)
        results.append(dict(zip(columns, fieldList)))
    return results


def add_data_link(session, current_row):
    """populate property, esource, data, total_link_counts, and data_links_rows fields"""

    q = config['PROPERTY_QUERY'].format(db='nonbib', bibcode=current_row['bibcode'])
    result = session.execute(q)
    current_row['property'] = fetch_data_link_elements(result.fetchone())

    q = config['ESOURCE_QUERY'].format(db='nonbib', bibcode=current_row['bibcode'])
    result = session.execute(q)
    current_row['esource'] = fetch_data_link_elements(result.fetchone())

    q = config['DATA_QUERY'].format(db='nonbib', bibcode=current_row['bibcode'])
    result = session.execute(q)
    current_row['data'],current_row['total_link_counts'] = fetch_data_link_elements_counts(result.fetchone())

    q = config['DATALINKS_QUERY'].format(db='nonbib', bibcode=current_row['bibcode'])
    result = session.execute(q)
    current_row['data_links_rows'] = fetch_data_link_record(result.fetchall())


def nonbib_to_master_pipeline(nonbib_engine, schema, batch_size=1):
    """send all nonbib data to queue for delivery to master pipeline"""
    global config
    Session = sessionmaker(bind=nonbib_engine)
    session = Session()
    session.execute('set search_path to {}'.format(schema))
    tmp = []
    i = 0
    max_rows = config['MAX_ROWS']
    q = session.query(models.NonBibTable).options(load_only(*nonbib_to_master_fields))
    for current_row in q.yield_per(100):
        current_row = nonbib_to_master_dict(current_row)
        add_data_link(session, current_row)
        rec = NonBibRecord(**current_row)
        tmp.append(rec._data)
        i += 1
        if max_rows > 0 and i >= max_rows:
            break
        if len(tmp) >= batch_size:
            recs = NonBibRecordList()
            recs.nonbib_records.extend(tmp)
            tmp = []
            logger.info("Calling 'app.forward_message' count = '%s'", i)
            task_output_results.delay(recs)

    if len(tmp) > 0:
        recs = NonBibRecordList()
        recs.nonbib_records.extend(tmp)
        logger.info("Calling 'app.forward_message' with count = '%s'", i)
        task_output_results.delay(recs)
    session.close()


def nonbib_delta_to_master_pipeline(nonbib_engine, schema, batch_size=1):
    """send data for changed bibcodes to master pipeline

    the delta table was computed by comparing to sets of nonbib data
    perhaps ingested on succesive days"""
    global config
    Session = sessionmaker(bind=nonbib_engine)
    session = Session()
    session.execute('set search_path to {}'.format(schema))
    tmp = []
    i = 0
    max_rows = config['MAX_ROWS']
    for current_delta in session.query(models.DeltaTable).yield_per(100):
        row = nonbib.get_by_bibcode(current_delta.bibcode, nonbib_to_master_fields)
        row = nonbib_to_master_dict(row)
        add_data_link(session, row)
        rec = NonBibRecord(**row)
        tmp.append(rec._data)
        i += 1
        if max_rows > 0 and i > max_rows:
            break
        if len(tmp) >= batch_size:
            recs = NonBibRecordList()
            recs.nonbib_records.extend(tmp)
            tmp = []
            logger.debug("Calling 'app.forward_message' with '%s' items", len(recs.nonbib_records))
            task_output_results.delay(recs)

    if len(tmp) > 0:
        recs = NonBibRecordList()
        recs.nonbib_records.extend(tmp)
        logger.debug("Calling 'app.forward_message' with final '%s' items", len(recs.nonbib_records))
        task_output_results.delay(recs)


def metrics_to_master_pipeline(metrics_engine, schema, batch_size=1):
    """send all metrics data to queue for delivery to master pipeline"""
    global config
    Session = sessionmaker(bind=metrics_engine)
    session = Session()
    session.execute('set search_path to {}'.format(schema))
    tmp = []
    i = 0
    max_rows = config['MAX_ROWS']
    for current_row in session.query(models.MetricsTable).yield_per(100):
        current_row = row2dict(current_row)
        current_row.pop('id')
        rec = MetricsRecord(**current_row)
        tmp.append(rec._data)
        i += 1
        if max_rows > 0 and i > max_rows:
            break
        if len(tmp) >= batch_size:
            recs = MetricsRecordList()
            recs.metrics_records.extend(tmp)
            logger.info("Calling metrics 'app.forward_message' count = '%s'", i)
            task_output_metrics.delay(recs)
            tmp = []

    if len(tmp) > 0:
        recs = MetricsRecordList()
        recs.metrics_records.extend(tmp)
        logger.debug("Calling metrics 'app.forward_message' with count = '%s'", i)
        task_output_metrics.delay(recs)


def metrics_delta_to_master_pipeline(metrics_engine, metrics_schema, nonbib_schema, batch_size=1):
    """send data for changed metrics to master pipeline

    the delta table was computed by comparing to sets of nonbib data
    perhaps ingested on succesive days"""
    m = metrics.Metrics(metrics_schema)
    n = nonbib.NonBib(nonbib_schema)
    connection = n.engine.connect()
    tmp = []
    for current_delta in session.query(models.DeltaTable).yield_per(100):
        row = m.read(current_delta.bibcode)
        rec = NonBibRecord(row2dict(row))
        rec.pop('id')
        rec = MetricsRecord(**dict(rec))
        tmp.append(rec._data)
        if len(recs.metrics_records) >= batch_size:
            recs = MetricsRecordList()
            recs.metrics_records.extend(tmp)
            logger.debug("Calling metrics 'app.forward_message' with '%s' messages", len(recs.metrics_records))
            task_output_metrics.delay(recs)
            tmp = []

    if len(tmp) > 0:
        recs = MetricsRecordList()
        recs.metrics_records.extend(tmp)
        logger.debug("Calling metrics 'app.forward_message' with final '%s'", str(rec))
        task_output_metrics.delay(recs)



def diagnose_nonbib():
    """send hard coded nonbib data the master pipeline

    useful for testing to verify connectivity"""

    test_data = {'bibcode': '2003ASPC..295..361M',
                 'simbad_objects': [], 'grants': ['g'], 'boost': 0.11,
                 'citation_count': 0, 'read_count': 2, 'readers': ['a', 'b'],
                 'reference': ['c', 'd']}
    recs = NonBibRecordList()
    rec = NonBibRecord(**test_data)
    recs.nonbib_records.extend([rec._data])
    print 'sending nonbib data for bibocde', test_data['bibcode'], 'to master pipeline'
    print 'using CELERY_BROKER', config['CELERY_BROKER']
    print '  CELERY_DEFAULT_EXCHANGE', config['CELERY_DEFAULT_EXCHANGE']
    print '  CELERY_DEFAULT_EXCHANGE_TYPE', config['CELERY_DEFAULT_EXCHANGE_TYPE']
    print '  OUTPUT_CELERY_BROKER', config['OUTPUT_CELERY_BROKER']
    print '  OUTPUT_TASKNAME', config['OUTPUT_TASKNAME']
    print 'this action did not use ingest database (configured at', config['INGEST_DATABASE'], ')'
    print '  or the metrics database (at', config['METRICS_DATABASE'], ')'
    task_output_results.delay(recs)


def diagnose_metrics():
    """send hard coded metrics data the master pipeline

    useful for testing to verify connectivity"""

    test_data = {'bibcode': '2003ASPC..295..361M', 'refereed':False, 'rn_citations': 0, 
                 'rn_citation_data': [], 
                 'downloads': [0,0,0,0,0,0,0,0,0,0,0,1,2,1,0,0,1,0,0,0,1,2],
                 'reads': [0,0,0,0,0,0,0,0,1,0,4,2,5,1,0,0,1,0,0,2,4,5], 
                 'an_citations': 0, 'refereed_citation_num': 0,
                 'citation_num': 0, 'reference_num': 0, 'citations': [], 'refereed_citations': [], 
                 'author_num': 2, 'an_refereed_citations': 0
                 }
    recs = MetricsRecordList()
    rec = MetricsRecord(**test_data)
    recs.metrics_records.extend([rec._data])
    print 'sending metrics data for bibocde', test_data['bibcode'], 'to master pipeline'
    print 'using CELERY_BROKER', config['CELERY_BROKER']
    print '  CELERY_DEFAULT_EXCHANGE', config['CELERY_DEFAULT_EXCHANGE']
    print '  CELERY_DEFAULT_EXCHANGE_TYPE', config['CELERY_DEFAULT_EXCHANGE_TYPE']
    print '  OUTPUT_CELERY_BROKER', config['OUTPUT_CELERY_BROKER']
    print '  OUTPUT_TASKNAME', config['OUTPUT_TASKNAME']
    print 'this action did not use ingest database (configured at', config['INGEST_DATABASE'], ')'
    print '  or the metrics database (at', config['METRICS_DATABASE'], ')'
    task_output_metrics.delay(recs)



    
def main():
    parser = argparse.ArgumentParser(description='process column files into Postgres')
    parser.add_argument('--fileType', default=None, help='all,downloads,simbad,etc.')
    parser.add_argument('-r', '--rowViewSchemaName', default='nonbib', help='name of the postgres row view schema')
    parser.add_argument('-m', '--metricsSchemaName', default='metrics', help='name of the postgres metrics schema')
    parser.add_argument('-b', '--rowViewBaselineSchemaName', default='nonbibstaging', 
                        help='name of old postgres schema, used to compute delta')
    parser.add_argument('-s', '--batchSize', default=100, 
                        help='used when queuing data')
    parser.add_argument('-d', '--diagnose', default=False, action='store_true', help='run simple test')
    parser.add_argument('command', default='help', nargs='?',
                        help='ingest | verify | createIngestTables | dropIngestTables | renameSchema ' \
                        + ' | createJoinedRows | createMetricsTable | dropMetricsTable ' \
                        + ' | populateMetricsTable | createDeltaRows | populateMetricsTableDelta ' \
                        + ' | runRowViewPipeline | runMetricsPipeline | createNewBibcodes ' \
                        + ' | runRowViewPipelineDelta | runMetricsPipelineDelta '\
                        + ' | runPipelines | runPipelinesDelta | nonbibToMasterPipeline | nonbibDeltaToMasterPipeline'
                        + ' | metricsToMasterPipeline')

    args = parser.parse_args()

    config.update(load_config())

    global logger
    logger = setup_logging('AdsDataSqlSync', config.get('LOG_LEVEL', 'INFO'))
    logger.info('starting AdsDataSqlSync.app with {}, {}'.format(args.command, args.fileType))
    nonbib_connection_string = config.get('INGEST_DATABASE',
                                   'postgresql://postgres@localhost:5432/postgres')
    nonbib_db_engine = create_engine(nonbib_connection_string)
    nonbib_db_conn = nonbib_db_engine.connect()

    metrics_connection_string = config.get('METRICS_DATABASE',
                                   'postgresql://postgres@localhost:5432/postgres')
    metrics_db_engine = create_engine(metrics_connection_string)
    metrics_db_conn = metrics_db_engine.connect()
    sql_sync = nonbib.NonBib(args.rowViewSchemaName)
    if args.command == 'help' and args.diagnose:
        diagnose_nonbib()
        diagnose_metrics()

    elif args.command == 'createIngestTables':
        sql_sync.create_column_tables(nonbib_db_engine)

    elif args.command == 'dropIngestTables':
        sql_sync.drop_column_tables(nonbib_db_engine)

    elif args.command == 'createJoinedRows':
        sql_sync.create_joined_rows(nonbib_db_conn)

    elif args.command == 'createMetricsTable' and args.metricsSchemaName:
        m = metrics.Metrics(args.metricsSchemaName)
        m.create_metrics_table(metrics_db_engine)

    elif args.command == 'dropMetricsTable' and args.metricsSchemaName:
        m = metrics.Metrics(args.metricsSchemaName)
        m.drop_metrics_table(metrics_db_engine)

    elif args.command == 'populateMetricsTable' and args.rowViewSchemaName and args.metricsSchemaName:
        m = metrics.Metrics()
        m.update_metrics_all(metrics_db_conn, nonbib_db_conn, args.rowViewSchemaName)

    elif args.command == 'populateMetricsTableDelta' and args.rowViewSchemaName and args.metricsSchemaName:
        m = metrics.Metrics(args.metricsSchemaName, {'FROM_SCRATCH': False, 'COPY_FROM_PROGRAM': False})
        m.update_metrics_changed(args.rowViewSchemaName)

    elif args.command == 'renameSchema' and args.rowViewSchemaName and args.rowViewBaselineSchemaName:
        sql_sync.rename_schema(nonbib_db_conn, args.rowViewBaselineSchemaName)

    elif args.command == 'createDeltaRows' and args.rowViewSchemaName and args.rowViewBaselineSchemaName:
        sql_sync.create_delta_rows(nonbib_db_conn, args.rowViewBaselineSchemaName)

    elif args.command == 'createNewBibcodes' and args.rowViewSchemaName and args.rowViewBaselineSchemaName:
        sql_sync.build_new_bibcodes(nonbib_db_conn, args.rowViewBaselineSchemaName)

    elif args.command == 'logDeltaReasons' and args.rowViewSchemaName and args.rowViewBaselineSchemaName:
        sql_sync.log_delta_reasons(nonbib_db_conn, args.rowViewBaselineSchemaName)

    elif args.command == 'runRowViewPipeline' and args.rowViewSchemaName:
        # drop tables, create tables, load data, create joined view
        sql_sync.drop_column_tables(nonbib_db_engine)
        sql_sync.create_column_tables(nonbib_db_engine)
        load_column_files(config, nonbib_db_engine, nonbib_db_conn, sql_sync)

    elif args.command == 'runMetricsPipeline' and args.rowViewSchemaName and args.metricsSchemaName:
        m = metrics.Metrics(args.metricsSchemaName)
        m.drop_metrics_table(metrics_db_engine)
        m.create_metrics_table(metrics_db_engine)
        m.update_metrics_all(metrics_db_conn, nonbib_db_conn, args.rowViewSchemaName)

    elif args.command == 'runRowViewPipelineDelta' and args.rowViewSchemaName and args.rowViewBaselineSchemaName:
        # read in flat files, compare to staging/baseline
        sql_sync.drop_column_tables(nonbib_db_engine)
        sql_sync.create_column_tables(nonbib_db_engine)
       
        load_column_files(config, nonbib_db_engine, nonbib_db_conn, sql_sync)

        sql_sync.create_delta_rows(nonbib_db_conn, args.rowViewBaselineSchemaName)
        sql_sync.log_delta_reasons(nonbib_db_conn, args.rowViewBaselineSchemaName)

    elif args.command == 'runMetricsPipelineDelta' and args.rowViewSchemaName and args.metricsSchemaName:
        m = metrics.Metrics(args.metricsSchemaName)
        m.update_metrics_changed(metrics_db_conn, nonbib_db_conn, args.rowViewSchemaName)

    elif args.command == 'runPipelines' and args.rowViewSchemaName and args.metricsSchemaName:
        # drop tables, create tables, load data, compute metrics
        sql_sync.drop_column_tables(nonbib_db_engine)
        sql_sync.create_column_tables(nonbib_db_engine)
        load_column_files(config, nonbib_db_engine, nonbib_db_conn, sql_sync)

        m = metrics.Metrics(args.metricsSchemaName)
        m.drop_metrics_table(metrics_db_engine)
        m.create_metrics_table(metrics_db_engine)
        m.update_metrics_all(metrics_db_conn, nonbib_db_conn, args.rowViewSchemaName)

    elif args.command == 'runPipelinesDelta' and args.rowViewSchemaName and args.metricsSchemaName and args.rowViewBaselineSchemaName:
        # drop tables, rename schema, create tables, load data, compute delta, compute metrics
        baseline_sql_sync = nonbib.NonBib(args.rowViewBaselineSchemaName)
        baseline_sql_sync.drop_column_tables()
        sql_sync.rename_schema(args.rowViewBaselineSchemaName)

        baseline_sql_sync = None
        sql_sync.create_column_tables(nonbib_db_engine)
       
        load_column_files(config, sql_sync)

        sql_sync.create_delta_rows(args.rowViewBaselineSchemaName)
        sql_sync.log_delta_reasons(args.rowViewBaselineSchemaName)

        m = metrics.Metrics(args.metricsSchemaName, {'FROM_SCRATCH': False, 'COPY_FROM_PROGRAM': False})
        m.update_metrics_changed(args.rowViewSchemaName)
    elif args.command == 'nonbibToMasterPipeline' and args.diagnose:
        diagnose_nonbib()
    elif args.command == 'nonbibToMasterPipeline':
        nonbib_to_master_pipeline(nonbib_db_engine, args.rowViewSchemaName, int(args.batchSize))
    elif args.command == 'nonbibDeltaToMasterPipeline':
        print 'diagnose = ', args.diagnose
        nonbib_delta_to_master_pipeline(nonbib_eb_engine, args.rowViewSchemaName, int(args.batchSize))
    elif args.command == 'metricsToMasterPipeline' and args.diagnose:
        diagnose_metrics()
    elif args.command == 'metricsToMasterPipeline':
        metrics_to_master_pipeline(metrics_db_engine, args.metricsSchemaName, int(args.batchSize))
    elif args.command == 'metricsDeltaToMasterPipeline':
        nonbib_delta_to_master_pipeline(metrics_db_engine, args.metricsSchemaName, args.rowViewSchemaName, int(args.batchSize))

    else:
        print 'app.py: illegal command or missing argument, command = ', args.command
        print '  row view schema name = ', args.rowViewSchemaName
        print '  row view baseline schema name = ', args.rowViewBaselineSchemaName
        print '  metrics schema name = ', args.metricsSchemaName

    if nonbib_db_conn:
        nonbib_db_conn.close()
            
    logger.info('completed columnFileIngest with {}, {}'.format(args.command, args.fileType))


if __name__ == "__main__":
    main()
