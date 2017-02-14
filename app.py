
import sys
import re
import argparse
import utils
import os
from sqlalchemy.orm import sessionmaker


import columnFileIngest
import row_view
import metrics
import queue
logger = None
config = {}


def load_column_files(sql_sync):
    """ generate and run sql 'copy from program' commands to load column files
    and, create joined row view 
    note that the copy from program command actually runs this python code with different arguments
    """
    Session = sessionmaker()
    sess = Session(bind=sql_sync.sql_sync_connection)
    for t in row_view.SqlSync.all_types:
        command_args = ' --fileType ' + t +   \
                       ' --rowViewSchemaName ' + sql_sync.schema_name + ' ingest'
        python_command = "'python " + os.path.abspath(__file__) + command_args + "'"
        sql_command = 'copy ' + sql_sync.schema_name + '.' + t +  \
                      ' from program ' + python_command + ';'

        sess.execute(sql_command)
        sess.commit()
    sess.close()
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
    parser.add_argument('-b', '--rowViewBaselineSchemaName', default='baseline', 
                        help='name of old postgres schema, used to compute delta')
    parser.add_argument('command', default='help', 
                        help='ingest | verify | createIngestTables | dropIngestTables | renameSchema ' \
                        + ' | createJoinedRows | ingestMeta | createMetricsTable | dropMetricsTable ' \
                        + ' | populateMetricsTable | populateMetricsTableMeta | createDeltaRows | populateMetricsTableDelta ' \
                        + ' | runRowViewPipeline | runMetricsPipeline ' \
                        + ' | runRowViewPipelineDelta | runMetricsPipelineDelta '\
                        + ' | queueChangedBibcodes | queueAllBibcodes | initQueue | testQueue | runPipelines | runPipelinesDelta')

    args = parser.parse_args()

    config.update(utils.load_config())

    global logger
    logger = utils.setup_logging('AdsDataSqlSync', 'AdsDataSqlSync', config['LOGGING_LEVEL'])
    logger.info('starting AdsDataSqlSync.app with {}, {}'.format(args.command, args.fileType))

    ingester = columnFileIngest.ColumnFileIngester(config)

    if args.command == 'ingest' and args.fileType == 'all':
        # all is only useful for testing, sending output to the console
        for t in row_view.SqlSync.all_types:
            print
            print t
            ingester.process_file(t)
    elif args.command == 'ingest' and args.fileType:
        ingester.process_file(args.fileType)

    elif args.command == 'verify' and args.fileType == 'all':
        for t in row_view.SqlSync.all_types:
            ingester.verify_file(t, args.rowViewSchemaName)
    elif args.command == 'verify' and args.fileType:
        ingester.verify_file(args.fileType, args.rowViewSchemaName)

    elif args.command == 'createIngestTables':
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
        sess = Session(bind=sql_sync.sql_sync_connection)
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
        m = metrics.Metrics(args.metricsSchemaName, {'FROM_SCRATCH': True})
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
    elif args.command == 'queueAllBibcodes' and args.rowViewSchemaName:
        q = queue.Queue(args.rowViewSchemaName, config)
        q.add_all_bibcodes()
    elif args.command == 'queueChangedBibcodes' and args.rowViewSchemaName:
        q = queue.Queue(args.rowViewSchemaName, config)
        q.add_changed_bibcodes()
    elif args.command == 'logDeltaReasons' and args.rowViewSchemaName and args.rowViewBaselineSchemaName:
        sql_sync = row_view.SqlSync(args.rowViewSchemaName, config)
        sql_sync.log_delta_reasons(args.rowViewBaselineSchemaName)
    elif args.command == 'initQueue':
        q = queue.Queue(None, config)
        q.init_rabbitmq()
    elif args.command == 'testQueue':
        q = queue.Queue(None, config)
        q.publish_to_rabbitmq(['1513hist.book.....H', '1057wjlf.book.....C'])
    elif args.command == 'runRowViewPipeline' and args.rowViewSchemaName:
        # drop tables, create tables, load data, compute metrics
        sql_sync = row_view.SqlSync(args.rowViewSchemaName, config)
        sql_sync.drop_column_tables()
        sql_sync.create_column_tables()
        load_column_files(sql_sync)

    elif args.command == 'runMetricsPipeline' and args.rowViewSchemaName and args.metricsSchemaName:
        m = metrics.Metrics(args.metricsSchemaName, {'COPY_FROM_PROGRAM': True})
        m.drop_metrics_table()
        m.create_metrics_table()
        load_metrics(m, args.rowViewSchemaName)


    elif args.command == 'runRowViewPipelineDelta' and args.rowViewSchemaName and args.rowViewBaselineSchemaName:
        # drop tables, rename schema, create tables, load data, compute delta, compute metrics
        baseline_sql_sync = row_view.SqlSync(args.rowViewBaselineSchemaName, config)
        baseline_sql_sync.drop_column_tables()
        sql_sync = row_view.SqlSync(args.rowViewSchemaName, config)
        sql_sync.rename_schema(args.rowViewBaselineSchemaName)

        baseline_sql_sync = None
        sql_sync = row_view.SqlSync(args.rowViewSchemaName, config)
        sql_sync.create_column_tables()
       
        load_column_files(sql_sync)

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
        load_column_files(sql_sync)


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
       
        load_column_files(sql_sync)

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
 
