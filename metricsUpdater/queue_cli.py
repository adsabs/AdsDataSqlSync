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
from metricsUpdater.app import app

# Code for updating the metrics rds-based Postgres instance
# it provides a cli to queue bibcodes
# This code is based on Celery usage in ADSimportpipeline/aip


# queue data from bibcode file with:
# $root@adsqb:/app/metricsUpdater# python queue_cli.py /tmp/bibcodes.test localhost metrics --to-vhost metrics


logger = get_task_logger('AdsDataSqlSync')
conf = utils.load_config()

def main():
    parser = argparse.ArgumentParser(description='Read metrics records and queue for writing by workers')
    parser.add_argument('bibcodes_filename', type=str, help='Filen with bibcodes')
    parser.add_argument('from_database', type=str, help='Database: localhost | test | prod')
    parser.add_argument('from_schema', type=str, help='Schema (e.g., metrics, metricstaging or "")')
    parser.add_argument('--to-vhost', default='metrics', help='Send updates to RabbitMQ vhost: metrics | metrics-test | metrics-prod')
    args = parser.parse_args()

    if args.from_database not in conf['METRICS_CONNECTIONS'].keys():
        raise Exception("Unknown database '{}'".format(args.from_database))

    filename = args.bibcodes_filename
    from_database_connection_string = conf['METRICS_CONNECTIONS'][args.from_database]
    from_schema = args.from_schema
    to_vhost = args.to_vhost
    print 'file {}, from database {}, from schema {}, to vhost {}'.format(filename, from_database_connection_string, from_schema, to_vhost)
    queue_metrics(filename, from_database_connection_string, from_schema, to_vhost)



def queue_metrics(bibcodes_filename, connection_string, from_schema, to_vhost):
    """
    Read list of changed bibcodes from file, get full records from database and queue them for delivery
    using the a RabbitMQ vhost (the workers listening will save to the correct destination database/schema).

    Full records come from the localhost Postgres database.
    """
    app.conf.BROKER_URL = "/".join(app.conf.BROKER_URL.split("/")[:-1] + [to_vhost,])
    logger.info("RabbitMQ vhost set to '{}'".format(to_vhost))

    logger.info('queuing bibcodes from file {}'.format(bibcodes_filename))
    routing_key = "update-metrics"
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



