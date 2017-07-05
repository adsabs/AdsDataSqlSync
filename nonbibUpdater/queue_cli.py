from __future__ import absolute_import, unicode_literals
from celery import Celery
from celery.utils.log import get_task_logger
from celery import Task
from celery.signals import worker_process_init
from celery.signals import worker_process_shutdown
from kombu import Exchange, Queue
import argparse

        
import sys
sys.path.append("..")
import utils
from row_view import SqlSync
from nonbibUpdater.app import task_update_nonbib

# Code for updating the nonbib rds-based Postgres instance
# it provides a cli to queue bibcodes
# This code is based on Celery usage in ADSimportpipeline/aip

# queue records from tmp (where they are initially created):
# python queue_cli.py -d localhost -s tmp -b /tmp/bibcodes.list.can

logger = get_task_logger('AdsDataSqlSync')
conf = utils.load_config()

def main():
    parser = argparse.ArgumentParser(description='read nonbib records and queue for writing by workers')
    parser.add_argument('-b', '--bibcodes', default='foo', help='file with bibcodes')
    parser.add_argument('-d', '--database', default='localhost', help='localhost')
    parser.add_argument('-s', '--schema', default='tmp', help='tmp')

    
    args = parser.parse_args()
    filename = args.bibcodes
    database = conf['NONBIB_CONNECTIONS'][args.database]
    schema = conf['NONBIB_SCHEMA_NAMES'][args.schema]
    print 'file {}, database {}, schema {}'.format(filename, database, schema)
    queue_nonbib(filename, database, schema)

def queue_nonbib(bibcodes_filename, database, schema):
    """read list of changed bibcodes from file, get full records from database and queue them for delivery
    full records come from the localhost Postgres database
    """
    logger.info('queuing full rowview records for bibcodes from file {}'.format(bibcodes_filename))
    nonbib_source =  SqlSync(schema, {'INGEST_DATABASE': database})
    bibcodes = []
    final_flag = False
    f = open(bibcodes_filename)
    if f is None:
        logger.error('could not open {}'.format(bibcodes_filename))
        return
    while True:
        line = f.readline()
        if len(line) == 0:
            final_flag = True
        else:
            bibcode = line.strip()
            bibcodes.append(bibcode)

        if len(bibcodes) >= 100 or final_flag:
            logger.info('time to save data for {} bibcodes'.format(len(bibcodes)))
            if len(bibcodes) > 0:
                db_records = nonbib_source.get_by_bibcodes(bibcodes)
                update_buffer = []
                for db_record in db_records:
                    update_buffer.append(dict(db_record))
                task_update_nonbib.delay(update_buffer)
            logger.info('added {} records to queue, out of {}'.format(len(update_buffer), len(bibcodes)))
            print 'added {} records to queue, out of {}'.format(len(update_buffer), len(bibcodes))
            bibcodes = []
            
        if final_flag:
            nonbib_source.sql_sync_connection.close()
            return



if __name__ == '__main__':
    main()

        
