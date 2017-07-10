import argparse
import sys
import re
import app as app_module
sys.path.append("../")
from row_view import SqlSync
import nonbibUpdater.tasks
from utils import queue_rows

   
def main():
    parser = argparse.ArgumentParser(description='Read nonbib records for and queue them for writing by workers')
    parser.add_argument('bibcodes_filename', type=str, help='File with bibcodes')
    args = parser.parse_args()
    queue_nonbib(args.bibcodes_filename)

    
def queue_nonbib(bibcodes_filename): 
    """
    Read list of changed bibcodes, get full records from database and queue them for delivery using the a RabbitMQ vhost 
    
    The workers listening will save to the correct destination database/schema.
    Full records come from the localhost Postgres database.
    """
    app_obj = app_module.NonbibUpdaterCelery('nonbib-updater')
    logger = app_obj.logger
    db_connection = app_obj.conf['NONBIB_READ_CONNECTION']
    db_schema = app_obj.conf['NONBIB_READ_SCHEMA']
    celery_connection = app_obj.conf['CELERY_BROKER']
    logger.info('read bibcodes from file {}, queue records from database {}, schema {}, on queue {}'
        .format(bibcodes_filename, re.sub('//.*@', '', db_connection), db_schema, re.sub('//.*@', '', celery_connection)))

    db_connection =  SqlSync(db_schema, {'INGEST_DATABASE': db_connection})
    queue_rows(bibcodes_filename, db_connection, nonbibUpdater.tasks.task_update_nonbib, logger)
    db_connection.connection.close()

    
if __name__ == "__main__":
    main()
