
from __future__ import absolute_import, unicode_literals

import sys, os
sys.path.append("..")
import re
from kombu import Queue
from celery.signals import worker_process_init, worker_process_shutdown, user_preload_options

from nonbibUpdater import app as app_module

from row_view import SqlSync

app = app_module.NonbibUpdaterCelery('nonbib-updater')
logger = app.logger
db_connection = None

app.conf.CELERY_QUEUES = (
    Queue('errors', app.exchange, routing_key='errors', durable=False, message_ttl=24*3600*5),
    Queue('update-nonbib', app.exchange, routing_key='update-nonbib'),
)

@app.task(queue="update-nonbib")
#def task_update_metrics(metrics_records):
def task_update_nonbib(nonbib_records):
    """receives a set of full non_bib records, updates nonbib Postgres instance                                         
                                                                                                                        
    nonbib record includes primary key index field, staging value will not match other database                         
    """
    global db_connection

    logger.info('in task_update_nonbib with {} records'.format(len(nonbib_records)))
    db_schema = app.conf['NONBIB_WRITE_SCHEMA']
    database_buffer = []
    for current in nonbib_records:
        bibcode = current['bibcode']
        r = db_connection.get_row_view(bibcode)
        if r:
            # here if there is an existing record in the database, we need to do sql update                                               
            current['id'] = r['id']
            current['tmp_bibcode'] = bibcode
            database_buffer.append(current)
        else:
            # here if this is a new record for rds                                                                                            
            # we do not cache inserts, they are a small percentage of the traffic                                                             
            db_connection.sql_sync_connection.execute(db_connection.table.insert(), [current])
            
    # for improved, performance we perform a single sql update operation                                                                      
    if len(database_buffer) > 0:
        db_connection.sql_sync_connection.execute(db_connection.u, database_buffer)
    logger.info('updated {} metrics records to schema {}'.format(len(database_buffer), db_schema))


@worker_process_init.connect
def init_worker(**kwargs):
    """Open a database connection that this worker can reuse
                                                                                                                                              
    Source: http://stackoverflow.com/questions/14526249/celery-worker-database-connection-pooling                                             
    """
    global db_connection

    db_connection = app.conf['NONBIB_WRITE_CONNECTION']
    db_schema = app.conf['NONBIB_WRITE_SCHEMA']
    
    logger.info('worker writing to database {}, schema {}'.format(re.sub('//.*@', '', db_connection), db_schema))
    db_connection = SqlSync(db_schema, {'INGEST_DATABASE': db_connection})



@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    """Close this workers open database connection when the worker ends."""
    global db_connection
    if db_connection:
        logger.info('Closing worker connection to database')
        db_connection.sql_sync_connection.close()
    


if __name__ == '__main__':
    app.start()
   

