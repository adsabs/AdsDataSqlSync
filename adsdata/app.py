
import sys
import re
import argparse
import os
from sqlalchemy.orm import sessionmaker

import row_view
import metrics
import reader
from adsputils import load_config, setup_logging

logger = None
config = {}

def test_load_column_files(sql_sync):
    """ use cursor/psycopg.copy_from to data from column file to postgres
    
    after data has been loaded, join to create a unified row view 
    """

    conn = sql_sync.engine.raw_connection()
    cur = conn.cursor()
    table_name = sql_sync.schema + '.' + 'canonical'
    f = reader.BibcodeFileReader('/Users/SpacemanSteve/tmp/bibcodes.list.can.20170522') #../tests/data/testBibcodes120.txt')
    f = reader.BibcodeFileReader('../tests/data/testBibcodes2.txt')
    cur.copy_from(f, table_name)
    conn.commit()
    table_name = sql_sync.schema + '.' + 'refereed'
    f = reader.RefereedFileReader('../tests/data/testBibcodes2.txt')
    cur.copy_from(f, table_name)
    conn.commit()
    table_name = sql_sync.schema + '.' + 'author'
    f = reader.StandardFileReader('author', '../tests/data/data1/facet_authors/all.links')
    cur.copy_from(f, table_name)
    conn.commit()
    cur.close()
    conn.close()

def load_column_files(config, sql_sync):
    """ use psycopg.copy_from to data from column file to postgres
    
    after data has been loaded, join to create a unified row view 
    """
    conn = sql_sync.engine.raw_connection()
    cur = conn.cursor()
    
    for t in row_view.SqlSync.all_types:
        table_name = sql_sync.schema + '.' + t
        logger.info('processing {}'.format(table_name))
        filename = config['DATA_PATH'] + config[t.upper()]
        if t == 'canonical':
            r = reader.BibcodeFileReader(filename)
        elif t == 'refereed':
            r = reader.RefereedFileReader(filename) 
        else:
            r = reader.StandardFileReader(t, filename)
        if r:
            cur.copy_from(r, table_name)
            conn.commit()

    cur.close()
    conn.close()
    sql_sync.create_joined_rows()

def load_metrics(m, row_view_schema):
    """
    """
    Session = sessionmaker()
    sess = Session(bind=m.connection)
    command_args = ' populateMetricsTable '  \
                   + ' --rowViewSchemaName ' + row_view_schema \
                   + ' --metricsSchemaName ' + m.schema
    python_command = "'python " + os.path.abspath(__file__) + command_args + "'"
    sql_command = 'copy ' + m.schema + '.metrics'   \
                  + ' from program ' + python_command + ';'
    
    sess.execute(sql_command)
    sess.commit()
    sess.close()



    
def main():
    parser = argparse.ArgumentParser(description='process column files into Postgres')
    parser.add_argument('--fileType', default=None, help='all,downloads,simbad,etc.')
    parser.add_argument('-r', '--rowViewSchemaName', default='nonbib', help='name of the postgres row view schema')
    parser.add_argument('-m', '--metricsSchemaName', default='metrics', help='name of the postgres metrics schema')
    parser.add_argument('-b', '--rowViewBaselineSchemaName', default='nonbibstaging', 
                        help='name of old postgres schema, used to compute delta')
    parser.add_argument('command', default='help', 
                        help='ingest | verify | createIngestTables | dropIngestTables | renameSchema ' \
                        + ' | createJoinedRows | ingestMeta | createMetricsTable | dropMetricsTable ' \
                        + ' | populateMetricsTable | populateMetricsTableMeta | createDeltaRows | populateMetricsTableDelta ' \
                        + ' | runRowViewPipeline | runMetricsPipeline | createNewBibcodes ' \
                        + ' | runRowViewPipelineDelta | runMetricsPipelineDelta '\
                        + ' | runPipelines | runPipelinesDelta')

    args = parser.parse_args()

    config.update(load_config())

    global logger
    logger = setup_logging('AdsDataSqlSync', config.get('LOG_LEVEL', 'INFO'))
    logger.info('starting AdsDataSqlSync.app with {}, {}'.format(args.command, args.fileType))

    if args.command == 'createIngestTables':
        sql_sync = row_view.SqlSync(args.rowViewSchemaName, config)
        sql_sync.create_column_tables()
    elif args.command == 'dropIngestTables':
        sql_sync = row_view.SqlSync(args.rowViewSchemaName, config)
        sql_sync.drop_column_tables()
    elif args.command == 'createJoinedRows':
        sql_sync = row_view.SqlSync(args.rowViewSchemaName, config)
        sql_sync.create_joined_rows()

    elif args.command == 'ingestMeta' and args.rowViewSchemaName:
        # run copy from program command
        # that sql command will invoke ingest of each table
        sql_sync = row_view.SqlSync(args.rowViewSchemaName, config)
        Session = sessionmaker()
        sess = Session(bind=sql_sync.connection)
        for t in row_view.SqlSync.all_types:
            command_args = ' ingest --fileType ' + t +   \
                ' --rowViewSchemaName ' + args.rowViewSchemaName
            python_command = "'python " + os.path.abspath(__file__) + command_args + "'"
            sql_command = 'copy ' + args.rowViewSchemaName + '.' + t +  \
                ' from program ' + python_command + ';'

            sess.execute(sql_command)
            sess.commit()

        sess.close()

    elif args.command == 'createMetricsTable' and args.metricsSchemaName:
        m = metrics.Metrics(args.metricsSchemaName)
        m.create_metrics_table()
    elif args.command == 'dropMetricsTable' and args.metricsSchemaName:
        m = metrics.Metrics(args.metricsSchemaName)
        m.drop_metrics_table()
    elif args.command == 'populateMetricsTable' and args.rowViewSchemaName and args.metricsSchemaName:
        # m = metrics.Metrics(None, {'FROM_SCRATCH': False, 'COPY_FROM_PROGRAM': False})
        m = metrics.Metrics(None, {'FROM_SCRATCH': True, 'COPY_FROM_PROGRAM': True})
        m.update_metrics_all(args.rowViewSchemaName)
    elif args.command == 'populateMetricsTableDelta' and args.rowViewSchemaName and args.metricsSchemaName:
        m = metrics.Metrics(args.metricsSchemaName, {'FROM_SCRATCH': False, 'COPY_FROM_PROGRAM': False})
        m.update_metrics_changed(args.rowViewSchemaName)
    elif args.command == 'populateMetricsTableMeta' and args.rowViewSchemaName and args.metricsSchemaName:
        # run copy from program command
        # that sql command will populate metrics table
        # create metrics to get a database connection for session
        m = metrics.Metrics(args.metricsSchemaName, {'COPY_FROM_PROGRAM': True})
        Session = sessionmaker()
        sess = Session(bind=m.connection)

        command_args = ' populateMetricsTable '  \
            + ' --rowViewSchemaName ' + args.rowViewSchemaName \
            + ' --metricsSchemaName ' + args.metricsSchemaName
        python_command = "'python " + os.path.abspath(__file__) + command_args + "'"
        sql_command = 'copy ' + args.metricsSchemaName + '.metrics'   \
            + ' from program ' + python_command + ';'

        sess.execute(sql_command)
        sess.commit()
        sess.close()
    elif args.command == 'renameSchema' and args.rowViewSchemaName and args.rowViewBaselineSchemaName:
        sql_sync = row_view.SqlSync(args.rowViewSchemaName, config)
        sql_sync.rename_schema(args.rowViewBaselineSchemaName)
    elif args.command == 'createDeltaRows' and args.rowViewSchemaName and args.rowViewBaselineSchemaName:
        sql_sync = row_view.SqlSync(args.rowViewSchemaName, config)
        sql_sync.create_delta_rows(args.rowViewBaselineSchemaName)
    elif args.command == 'createNewBibcodes' and args.rowViewSchemaName and args.rowViewBaselineSchemaName:
        sql_sync = row_view.SqlSync(args.rowViewSchemaName, config)
        sql_sync.build_new_bibcodes(args.rowViewBaselineSchemaName)
    elif args.command == 'logDeltaReasons' and args.rowViewSchemaName and args.rowViewBaselineSchemaName:
        sql_sync = row_view.SqlSync(args.rowViewSchemaName, config)
        sql_sync.log_delta_reasons(args.rowViewBaselineSchemaName)
    elif args.command == 'test':
        sql_sync = row_view.SqlSync(args.rowViewSchemaName, config)
        sql_sync.drop_column_tables()
        sql_sync.create_column_tables()
        test_load_column_files(sql_sync)
    elif args.command == 'runRowViewPipeline' and args.rowViewSchemaName:
        # drop tables, create tables, load data, compute metrics
        sql_sync = row_view.SqlSync(args.rowViewSchemaName, config)
        sql_sync.drop_column_tables()
        sql_sync.create_column_tables()
        load_column_files(config, sql_sync)

    elif args.command == 'runMetricsPipeline' and args.rowViewSchemaName and args.metricsSchemaName:
        m = metrics.Metrics(args.metricsSchemaName, {'COPY_FROM_PROGRAM': True})
        m.drop_metrics_table()
        m.create_metrics_table()
        load_metrics(m, args.rowViewSchemaName)


    elif args.command == 'runRowViewPipelineDelta' and args.rowViewSchemaName and args.rowViewBaselineSchemaName:
        # read in flat files, compare to staging/baseline
        sql_sync = row_view.SqlSync(args.rowViewSchemaName, config)

        sql_sync = row_view.SqlSync(args.rowViewSchemaName, config)
        sql_sync.drop_column_tables()
        sql_sync.create_column_tables()
       
        load_column_files(config, sql_sync)

        sql_sync.create_delta_rows(args.rowViewBaselineSchemaName)
        sql_sync.log_delta_reasons(args.rowViewBaselineSchemaName)

    elif args.command == 'runMetricsPipelineDelta' and args.rowViewSchemaName and args.metricsSchemaName:

        m = metrics.Metrics(args.metricsSchemaName, {'FROM_SCRATCH': False, 'COPY_FROM_PROGRAM': False})
        m.update_metrics_changed(args.rowViewSchemaName)


    elif args.command == 'runPipelines' and args.rowViewSchemaName and args.metricsSchemaName:
        # drop tables, create tables, load data, compute metrics
        sql_sync = row_view.SqlSync(args.rowViewSchemaName, config)
        sql_sync.drop_column_tables()
        sql_sync.create_column_tables()
        load_column_files(config, sql_sync)

        m = metrics.Metrics(args.metricsSchemaName, {'COPY_FROM_PROGRAM': True})
        m.drop_metrics_table()
        m.create_metrics_table()
        load_metrics(m, args.rowViewSchemaName)


    elif args.command == 'runPipelinesDelta' and args.rowViewSchemaName and args.metricsSchemaName and args.rowViewBaselineSchemaName:
        # drop tables, rename schema, create tables, load data, compute delta, compute metrics
        baseline_sql_sync = row_view.SqlSync(args.rowViewBaselineSchemaName, config)
        baseline_sql_sync.drop_column_tables()
        sql_sync = row_view.SqlSync(args.rowViewSchemaName, config)
        sql_sync.rename_schema(args.rowViewBaselineSchemaName)

        baseline_sql_sync = None
        sql_sync = row_view.SqlSync(args.rowViewSchemaName, config)
        sql_sync.create_column_tables()
       
        load_column_files(config, sql_sync)

        sql_sync.create_delta_rows(args.rowViewBaselineSchemaName)
        sql_sync.log_delta_reasons(args.rowViewBaselineSchemaName)

        m = metrics.Metrics(args.metricsSchemaName, {'FROM_SCRATCH': False, 'COPY_FROM_PROGRAM': False})
        m.update_metrics_changed(args.rowViewSchemaName)


    else:
        print 'app.py: illegal command or missing argument, command = ', args.command
        print '  row view schema name = ', args.rowViewSchemaName
        print '  row view baseline schema name = ', args.rowViewBaselineSchemaName
        print '  metrics schema name = ', args.metricsSchemaName

            
    logger.info('completed columnFileIngest with {}, {}'.format(args.command, args.fileType))


if __name__ == "__main__":
    main()
