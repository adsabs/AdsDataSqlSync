from __future__ import absolute_import, unicode_literals
from celery import Celery
from celery.utils.log import get_task_logger
from celery import Task
from celery.signals import worker_process_init
from celery.signals import worker_process_shutdown
from celery import bootsteps
from celery.bin import Option

from kombu import Exchange, Queue


        
import sys
sys.path.append("..")
import utils
from row_view import SqlSync

# Code for updating the nonbib row view 
# This code is based on Celery usage in ADSimportpipeline/aip

# run the workers with:
# root@adsqb:/app# celery -A nonbibUpdater.app worker -l info --concurrency=5
# root@adsqb:/app# celery -A nonbibUpdater.app worker -l info --concurrency=5  --schema=staging --database=localhost
# queue data from bibcode using queue_cli

logger = get_task_logger('AdsDataSqlSync')
conf = utils.load_config()

global database_connection_string
global schema_name

# celery configuration mostly from ADSimportpipeline/aip/app.py
app = Celery('AdsDataSqlSyncStaging',
             broker=conf.get('CELERY_BROKER', 'pyamqp://'),
             include=['nonbibUpdater.app'])


exch = Exchange(conf.get('CELERY_DEFAULT_EXCHANGE', 'import-pipeline'), type=conf.get('CELERY_DEFAULT_EXCHANGE_TYPE', 'topic'))

app.conf.CELERY_QUEUES = (
    Queue('errors', exch, routing_key='errors', durable=False, message_ttl=24*3600*5),
    Queue('update-nonbib-staging', exch, routing_key='update-nonbib-staging')
)



# command line args based on http://docs.celeryproject.org/en/latest/userguide/extending.html#adding-new-command-line-options
# tell workers where to write to
def add_worker_arguments(parser):
    logger.info('adding worker arguments')
    parser.add_argument(
        '--schema', action='store', default='staging',
        help='schema identifier: staging')
    parser.add_argument(
        '--database', action='store', default='localhost',
        help='database identifier: localhost',
    ),
app.user_options['worker'].add(add_worker_arguments)

class MyBootstep(bootsteps.Step):
    def __init__(self, worker, database='localhost', schema='staging', **options):
        global database_connection_string
        database_connection_string = conf['NONBIB_CONNECTIONS'][database]
        global schema_name
        schema_name = conf['NONBIB_SCHEMA_NAMES'][schema]
        logger.info('in MyBootstep nonbib with database {} and schema {}, {} and {}'.format(
            database_connection_string, schema_name, database, schema))
        
app.steps['worker'].add(MyBootstep)
                                                                    
app.conf.update(conf)


class MyTask(Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
            logger.error('{0!r} failed: {1!r}'.format(task_id, exc))


@app.task(base=MyTask, queue='bar')
def task_foo(a, b):
    print a, b, a+b
    return a+b


                    
@app.task(base=MyTask, queue='update-nonbib-staging')
def task_update_nonbib(nonbib_records):
    """receives a set of full non_bib records, updates nonbib Postgres instance
    
    nonbib record includes primary key index field, staging value will not match other database
    """
    global nonbib_sink
    logger.info('in task_update_nonbib with {} records and db {}'.format(len(nonbib_records), nonbib_sink))
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
    global database_connection_string
    global schema_name
    logger.info('in init_worker nonbib, creating database')
    nonbib_sink = SqlSync(schema_name, {'INGEST_DATABASE': database_connection_string})


@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    global nonbib_sink
    if nonbib_sink:
        nonbib_sink.sql_sync_connection.close()
        logger.info('in nonbib shutdown_worker, closed database connection')

        


if __name__ == '__main__':
    app.start()

        
