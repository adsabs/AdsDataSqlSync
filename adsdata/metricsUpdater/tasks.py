
from __future__ import absolute_import, unicode_literals

import sys, os
sys.path.append("..")
import re
from kombu import Queue
from celery.signals import worker_process_init, worker_process_shutdown, user_preload_options

from metricsUpdater import app as app_module
from metrics import Metrics
from utils import process_rows

app = app_module.MetricsUpdaterCelery('metrics-updater')
logger = app.logger
metrics_rds = None


app.conf.CELERY_QUEUES = (
    Queue('errors', app.exchange, routing_key='errors', durable=False, message_ttl=24*3600*5),
    Queue('update-metrics', app.exchange, routing_key='update-metrics'),
)

@app.task(queue="update-metrics")
def task_update_metrics(metrics_records):
    """Receives a set of full metrics records and updates metrics Postgres table
                      
    """
    global metrics_rds
    logger.info('in task_update_metrics with {} records'.format(len(metrics_records)))
    process_rows(metrics_records, metrics_rds, logger)
    
    
@worker_process_init.connect
def init_worker(**kwargs):
    """Open a database connection that this worker can reuse
                                                                                                                                              
    Source: http://stackoverflow.com/questions/14526249/celery-worker-database-connection-pooling                                             
    """
    global metrics_rds

    logger = app.logger
    db_connection = app.conf['METRICS_WRITE_CONNECTION']
    db_schema = app.conf['METRICS_WRITE_SCHEMA']
    
    logger.info('worker writing to database {}, schema {}'.format(re.sub('//.*@', '', db_connection), db_schema))
    metrics_rds = Metrics(db_schema, {'METRICS_DATABASE': db_connection})



@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    """Close this workers open database connection when the worker ends."""
    global metrics_rds
    if metrics_rds:
        logger.info('Closing worker connection to database')
        metrics_rds.connection.close()
    


if __name__ == '__main__':
    app.start()


