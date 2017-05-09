from __future__ import absolute_import, unicode_literals
from celery import Celery
from celery.utils.log import get_task_logger
from celery import Task
from celery.signals import worker_process_init
from celery.signals import worker_process_shutdown
from celery import bootsteps
from celery.bin import Option
from celery import current_app

from kombu import Exchange, Queue
        
import sys
sys.path.append("..")
import utils
from metrics import Metrics

# Code for updating the metrics rds-based Postgres instance
# This code is based on Celery usage in ADSimportpipeline/aip

# run the workers with:
# root@adsqb:/app# celery -A metricsUpdater.app worker -l info --concurrency=5
# root@adsqb:/app# celery -A metricsUpdater.app worker -l info --concurrency=5  --schema=staging --database=localhost
# queue data from bibcode using queue_cli

logger = get_task_logger('AdsDataSqlSync')
conf = utils.load_config()

global metrics_rds
global database_connection_string
global schema_name

# celery configuration mostly from ADSimportpipeline/aip/app.py
app = Celery('AdsDataSqlSync',
             broker=conf.get('CELERY_BROKER', 'pyamqp://'),
             include=['metricsUpdater.app'])

exch = Exchange(conf.get('CELERY_DEFAULT_EXCHANGE', 'import-pipeline'), type=conf.get('CELERY_DEFAULT_EXCHANGE_TYPE', 'topic'))

app.conf.CELERY_QUEUES = (
    Queue('errors', exch, routing_key='errors', durable=False, message_ttl=24*3600*5),
    Queue('update-metrics', exch, routing_key='update-metrics')
)



# command line args based on http://docs.celeryproject.org/en/latest/userguide/extending.html#adding-new-command-line-options
# tell workers where to write to
def add_worker_arguments(parser):
    logger.info('adding worker arguments')
    parser.add_argument(
        '--schema', action='store', default='staging',
        help='schema identifier: tmp | staging | rds')
    parser.add_argument(
        '--database', action='store', default='staging',
        help='database identifier: localhost | test | prod',    
        
    ),
app.user_options['worker'].add(add_worker_arguments)

class MyBootstep(bootsteps.Step):
    def __init__(self, worker, database='localhost', schema='staging', **options):
        global database_connection_string
        database_connection_string = conf['METRICS_CONNECTIONS'][database]
        global schema_name
        schema_name = conf['METRICS_SCHEMA_NAMES'][schema]
        logger.info('in MyBootstep with database {} and schema {}, {} and {}'.format(database_connection_string, schema_name, database, schema))
            
app.steps['worker'].add(MyBootstep)


app.conf.update(conf)

                                    
class MyTask(Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
            logger.error('{0!r} failed: {1!r}'.format(task_id, exc))


@app.task(base=MyTask, queue='bar')
def task_foo(a, b):
    print a, b, a+b
    return a+b


                    
@app.task(base=MyTask, queue='update-metrics')
def task_update_metrics(metrics_records):
    """receives a set of full metrics records, updates rds based metrics Postgres instance
    
    metrics record includes primary key index field, staging value will not match rds database
    """
    
    global metrics_rds
    logger.info('in task_update_metrics with {} records'.format(len(metrics_records)))
    database_buffer = []
    for current in metrics_records:
        bibcode = current['bibcode']
        # create and populate new dict with info from passed param
        save_dict = {'bibcode': bibcode}
        copy_fields = ('refereed', 'rn_citations', 'rn_citation_data', 'rn_citations_hist', 'downloads',
                       'reads', 'an_citations', 'refereed_citation_num', 'citation_num',
                       'reference_num', 'citations', 'refereed_citations', 'author_num',
                       'an_refereed_citations', 'modtime')
        for field in copy_fields:
            save_dict[field] = current[field]

        #x = save_dict['reads']
        #x[-1] += 1
        #logger.warn('hacked reads {}'.format(x[-1]))
        
        # read existing record from rds
        r = metrics_rds.read(bibcode)
        if r:
            # here if there is an existing record in the rds database, we need to do sql update
            save_dict['id'] = r['id']
            save_dict['tmp_bibcode'] = bibcode
            database_buffer.append(save_dict)
        else:
            # here if this is a new record for rds
            # we do not cache inserts, they are a small percentage of the traffic
            save_dict.pop('id', None)
            logger.info('performing rds insert for {}'.format(bibcode))
            metrics_rds.connection.execute(metrics_rds.table.insert(), [save_dict])

    # for improved, performance we perform a single sql update operation
    if len(database_buffer) > 0:
        metrics_rds.connection.execute(metrics_rds.u, database_buffer)
    logger.info('updated {} metrics records'.format(len(database_buffer)))
    
            


metrics_rds = None

# use one database conection per worker
# http://stackoverflow.com/questions/14526249/celery-worker-database-connection-pooling
@worker_process_init.connect
def init_worker(**kwargs):
    """ open database connection to write new records"""
    global metrics_rds
    global database_connection_string
    global schema_name
    logger.info('in init_worker, creating database')
    print 'in init_worker, database {}, schema {}'.format(database_connection_string, schema_name)
    metrics_rds = Metrics(schema_name, {'METRICS_DATABASE': database_connection_string})


@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    global metrics_rds
    if metrics_rds:
        metrics_rds.connection.close()
        logger.info('in shutdown_worker, closed database connection')

        
if __name__ == '__main__':
    app.start()

        
