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
from metrics import Metrics
from metricsUpdater.app import task_update_metrics

# Code for updating the metrics rds-based Postgres instance
# it provides a cli to queue bibcodes
# This code is based on Celery usage in ADSimportpipeline/aip


# queue data from bibcode file with:
# $root@adsqb:/app/metricsUpdater# python queue_cli.py -s staging -d localhost -b /tmp/bibcodes.list.can.20170508.txt


logger = get_task_logger('AdsDataSqlSync')
conf = utils.load_config()

def main():
    parser = argparse.ArgumentParser(description='read metrics records and queue for writing by workers')
    parser.add_argument('-b', '--bibcodes', default='foo', help='file with bibcodes')
    parser.add_argument('-d', '--database', default='localhost', help='localhost | test | prod')
    parser.add_argument('-s', '--schema', default='tmp', help='tmp | staging | rds')
    
    args = parser.parse_args()
    filename = args.bibcodes
    database = conf['METRICS_CONNECTIONS'][args.database]
    schema = conf['METRICS_SCHEMA_NAMES'][args.schema]
    print 'file {}, database {}, schema {}'.format(filename, database, schema)
    queue_metrics(filename, database, schema)

    
        
def queue_metrics(bibcodes_filename, connection_string, schema):
    """read list of changed bibcodes from file, get full records from database and queue them for delivery
    full records come from the localhost Postgres database
    """
    logger.info('queuing bibcodes from file {}'.format(bibcodes_filename))
    metrics_db =  Metrics(schema, {'METRICS_DATABASE': connection_string})
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
            logger.info('time to queue metrics data for {} bibcodes'.format(len(bibcodes)))
            if len(bibcodes) > 0:
                db_records = metrics_db.get_by_bibcodes(bibcodes)
                update_buffer = []
                for db_record in db_records:
                    update_buffer.append(dict(db_record))
                task_update_metrics.delay(update_buffer)
            logger.info('added {} metrics records to queue, out of {}'.format(len(update_buffer), len(bibcodes)))
            bibcodes = []
            
        if final_flag:
            metrics_db.connection.close()
            return



if __name__ == "__main__":
    main()


        
