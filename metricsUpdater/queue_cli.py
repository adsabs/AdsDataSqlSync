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
# $root@adsqb:/app/metricsUpdater# python queue_cli.py -b /tmp/bibcodes.test --from-database localhost --from-schema tmp --to-database localhost --to-schema staging


logger = get_task_logger('AdsDataSqlSync')
conf = utils.load_config()

def main():
    parser = argparse.ArgumentParser(description='read metrics records and queue for writing by workers')
    parser.add_argument('-b', '--bibcodes', default='foo', help='file with bibcodes')
    parser.add_argument('-d', '--from-database', default='localhost', help='localhost | test | prod')
    parser.add_argument('-s', '--from-schema', default='tmp', help='tmp | staging | rds')
    parser.add_argument('-t', '--to-database', default='localhost', help='localhost | test | prod')
    parser.add_argument('-z', '--to-schema', default='tmp', help='tmp | staging | rds')
    args = parser.parse_args()

    for database in (args.from_database, args.to_database):
        if database not in conf['METRICS_CONNECTIONS'].keys():
            raise Exception("Unknown database '{}'".format(database))

    for schema in (args.from_schema, args.to_schema):
        if schema not in conf['METRICS_SCHEMA_NAMES'].keys():
            raise Exception("Unknown schema '{}'".format(schema))

    filename = args.bibcodes
    from_database = conf['METRICS_CONNECTIONS'][args.from_database]
    from_schema = conf['METRICS_SCHEMA_NAMES'][args.from_schema]
    to_database = args.to_database
    to_schema = args.to_schema
    print 'file {}, from database {}, from schema {}, to database {}, to schema {}'.format(filename, from_database, from_schema, to_database, to_schema)
    queue_metrics(filename, from_database, from_schema, to_database, to_schema)



def queue_metrics(bibcodes_filename, connection_string, from_schema, to_database, to_schema):
    """
    Read list of changed bibcodes from file, get full records from database and queue them for delivery
    using the a routing key that specifies the destination database/schema.

    Full records come from the localhost Postgres database.
    """
    logger.info('queuing bibcodes from file {}'.format(bibcodes_filename))
    routing_key = "update-metrics.{}.{}".format(to_database, to_schema)
    metrics_db =  Metrics(from_schema, {'METRICS_DATABASE': connection_string})
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
                task_update_metrics.apply_async(args=(update_buffer,), exchange='update-metrics', routing_key=routing_key)
            logger.info('added {} metrics records to queue, out of {}'.format(len(update_buffer), len(bibcodes)))
            bibcodes = []

        if final_flag:
            metrics_db.connection.close()
            return



if __name__ == "__main__":
    main()



