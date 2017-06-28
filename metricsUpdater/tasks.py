
from __future__ import absolute_import, unicode_literals

import sys, os
sys.path.append("..")
import re
from kombu import Queue
from celery.signals import worker_process_init, worker_process_shutdown, user_preload_options

from metricsUpdater import app as app_module

from metrics import Metrics

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
    Metrics record may include primary key index field which should not be saved to database
    """
    #v_routing_key = self.request.delivery_info['routing_key']                                                                                

    global metrics_rds

    logger.info('in task_update_metrics with {} records'.format(len(metrics_records)))
    db_schema = app.conf['METRICS_WRITE_SCHEMA']
    database_buffer = []
    for current in metrics_records:
        bibcode = current['bibcode']

        # read existing record from rds                                                                                                       
        r = metrics_rds.read(bibcode)
        if r:
            # here if there is an existing record in the rds database, we need to do sql update                                               
            current['id'] = r['id']
            current['tmp_bibcode'] = bibcode
            database_buffer.append(current)
        else:
            # here if this is a new record for rds                                                                                            
            # we do not cache inserts, they are a small percentage of the traffic                                                             
            logger.info('performing rds insert for {}'.format(bibcode))
            metrics_rds.connection.execute(metrics_rds.table.insert(), [current])

    # for improved, performance we perform a single sql update operation                                                                      
    if len(database_buffer) > 0:
        metrics_rds.connection.execute(metrics_rds.u, database_buffer)
    logger.info('updated {} metrics records to schema {}'.format(len(database_buffer), db_schema))
    
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


