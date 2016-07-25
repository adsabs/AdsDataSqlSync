
import sys
import re
import argparse
import utils
from sqlalchemy.orm import sessionmaker


from sqlSync import columnFileIngest
from sqlSync import row_view
logger = None
config = {}

def main():
    parser = argparse.ArgumentParser(description='process column files into Postgres')
    parser.add_argument('--fileType', default=None, help='all,downloads,simbad,etc.')
    parser.add_argument('--schemaName', default='public', help='name of the postgres schema, needed for verify')
    parser.add_argument('command', help='ingest | verify | createIngestTables | dropIngestTables')

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
            ingester.verify_file(t, args.schemaName)
    elif args.command == 'verify' and args.fileType:
        ingester.verify_file(args.fileType, args.schemaName)

    elif args.command == 'createIngestTables':
        sql_sync = row_view.SqlSync(config)
        sql_sync.create_column_tables()
    elif args.command == 'dropIngestTables':
        sql_sync = row_view.SqlSync(config)
        sql_sync.drop_column_tables()

    logger.info('completed columnFileIngest with {}, {}'.format(args.command, args.fileType))


if __name__ == "__main__":
    main()
