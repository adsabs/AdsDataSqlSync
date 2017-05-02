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

# Code for updating the nonbib row view 
# This code is based on Celery usage in ADSimportpipeline/aip

# run the workers with:
# root@adsqb:/app# celery -A localToStaging/app worker -l info --concurrency=5
# queue data from bibcode file with:
# >>> import localToStaging.app
# >>> localToStaging.app.queue_bibcodes('/tmp/testBibcodes1.txt')

logger = get_task_logger('AdsDataSqlSync')
conf = utils.load_config()


# celery configuration mostly from ADSimportpipeline/aip/app.py
app = Celery('AdsDataSqlSyncStaging',
             broker=conf.get('CELERY_BROKER', 'pyamqp://'),
             include=['localToStaging.app'])


exch = Exchange(conf.get('CELERY_DEFAULT_EXCHANGE', 'import-pipeline'), type=conf.get('CELERY_DEFAULT_EXCHANGE_TYPE', 'topic'))

app.conf.CELERY_QUEUES = (
    Queue('errors', exch, routing_key='errors', durable=False, message_ttl=24*3600*5),
    Queue('update-nonbib-staging', exch, routing_key='update-nonbib-staging')
)

app.conf.update(conf)


class MyTask(Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
            logger.error('{0!r} failed: {1!r}'.format(task_id, exc))


@app.task(base=MyTask, queue='bar')
def task_foo(a, b):
    print a, b, a+b
    return a+b


                    
@app.task(base=MyTask, queue='update-nonbib-staging')
def task_update_nonbib_staging(nonbib_records):
    """receives a set of full non_bib records, updates nonbib Postgres instance
    
    nonbib record includes primary key index field, staging value will not match other database
    """
    global nonbib_sink
    logger.info('in task_update_nonbib_staging with {} records and db {}'.format(len(nonbib_records), nonbib_sink))
    database_buffer = []
    for current in nonbib_records:
        # logger.info('current queued record = {}'.format(current['bibcode']))
        bibcode = current['bibcode']
        # create and populate new dict with info from passed param
        save_dict = {'bibcode': bibcode}
        copy_fields = ('authors', 'refereed', 'simbad_objects', 'grants', 'citations',
                       'boost', 'citation_count', 'read_count', 'norm_cites', 'readers',
                       'downloads', 'reference', 'reads')
                        
        for field in copy_fields:
            save_dict[field] = current[field]

        
        # read existing record from sink
        r = nonbib_sink.get_row_view(bibcode)
        if r:
            # here if there is an existing record in the database, we need to do sql update
            save_dict['id'] = r['id']
            save_dict['tmp_bibcode'] = bibcode
            database_buffer.append(save_dict)
        else:
            # here if this is a new record for rds
            # we do not cache inserts, they are a small percentage of the traffic
            save_dict.pop('id', None)
            logger.warn('performing rds insert for {}'.format(bibcode))
            nonbib_sink.sql_sync_connection.execute(nonbib_sink.table.insert(), [save_dict])

    logger.info('completed reading bibcodes, {} to save'.format(len(database_buffer)))
    # for improved, performance we perform a single sql update operation
    if len(database_buffer) > 0:
        nonbib_sink.sql_sync_connection.execute(nonbib_sink.u, database_buffer)
    logger.info('updated {} rowview records'.format(len(database_buffer)))
    
            


nonbib_sink = None

# use one database conection per worker
# http://stackoverflow.com/questions/14526249/celery-worker-database-connection-pooling
@worker_process_init.connect
def init_worker(**kwargs):
    global nonbib_sink
    logger.info('in init_worker, creating database')
    nonbib_sink = SqlSync('nonbibtest2', {'INGEST_DATABASE': 'postgresql://postgres@localhost:5432/postgres'})


@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    global nonbib_sink
    if nonbib_sink:
        logger.info('in shutdown_worker, should we close database?')

        
def queue_bibcodes(bibcodes_filename):
    """read list of changed bibcodes from file, get full records from database and queue them for delivery
    full records come from the localhost Postgres database
    """
    logger.info('queuing bibcodes from file {}'.format(bibcodes_filename))
    nonbib_source =  SqlSync('nonbib', {'INGEST_DATABASE': 'postgresql://postgres@localhost:5432/postgres'})
    update_buffer = []
    f = open(bibcodes_filename)
    if f is None:
        logger.error('could not open {}'.format(bibcodes_filename))
        return
    while True:
        line = f.readline()
        if len(line) == 0:
            logger.info('final saving {} records'.format(len(update_buffer)))
            if len(update_buffer) > 0:
                task_update_nonbib_staging.delay(update_buffer)
            return
        bibcode = line.strip()
        update_current = nonbib_source.get_row_view(bibcode)
        if update_current:
            d = dict(update_current.items())
            update_buffer.append(d)
            if len(update_buffer) >= 100:
                task_update_nonbib_staging.delay(update_buffer)
                update_buffer = []
        else:
            logger.info('bibcode has no nonbib entry {}'.format(bibcode))



if __name__ == '__main__':
    app.start()

        
