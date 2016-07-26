
import sys
import re
import argparse
import utils
import os
from sqlalchemy.orm import sessionmaker


from sqlSync import columnFileIngest
from sqlSync import row_view
from metrics import metrics
logger = None
config = {}

def main():
    parser = argparse.ArgumentParser(description='process column files into Postgres')
    parser.add_argument('--fileType', default=None, help='all,downloads,simbad,etc.')
    parser.add_argument('--rowViewSchemaName', default='public', help='name of the postgres schema, needed for verify')
    parser.add_argument('--metricsSchemaName', default='public', help='name of the postgres schema, needed for verify')
    parser.add_argument('command', default='help', help='ingest | verify | createIngestTables | dropIngestTables | \
createJoinedRows | createMetricsTable | dropMetricsTable')

    args = parser.parse_args()


    config.update(utils.load_config())

    global logger
    logger = utils.setup_logging(config['LOG_FILENAME'], 'AdsDataSqlSync', config['LOGGING_LEVEL'])
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
        print 'createJoinedRows'
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

    elif args.command == 'createMetricsTable':
        m = metrics.Metrics(args.metricsSchemaName)
        m.create_metrics_table()
    elif args.command == 'dropMetricsTable':
        m = metrics.Metrics(args.metricsSchemaName)
        m.drop_metrics_table()
    elif args.command == 'populateMetricsTable':
        m = metrics.Metrics(args.metricsSchemaName, {'FROM_SCRATCH': True})
        m.update_metrics_all(args.rowViewSchemaName)
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

    else:
        print 'app.py: illegal command', args.command

            
    logger.info('completed columnFileIngest with {}, {}'.format(args.command, args.fileType))


if __name__ == "__main__":
    main()
