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

# Code for updating the metrics rds-based Postgres instance
# This code is based on Celery usage in ADSimportpipeline/aip

# run the workers with:
# root@adsqb:/app# celery -A metricsUpdater.app worker -l info --concurrency=5
# queue data from bibcode file with:
# >>> import metricsUpdater.app
# >>> metricsUpdater.app.queue_metrics('/tmp/testBibcodes1.txt')

logger = get_task_logger('AdsDataSqlSync')
conf = utils.load_config()


# celery configuration mostly from ADSimportpipeline/aip/app.py
app = Celery('AdsDataSqlSync',
             broker=conf.get('CELERY_BROKER', 'pyamqp://'),
             include=['metricsUpdater.app'])


exch = Exchange(conf.get('CELERY_DEFAULT_EXCHANGE', 'import-pipeline'), type=conf.get('CELERY_DEFAULT_EXCHANGE_TYPE', 'topic'))

app.conf.CELERY_QUEUES = (
    Queue('errors', exch, routing_key='errors', durable=False, message_ttl=24*3600*5),
    Queue('update-metrics', exch, routing_key='update-metrics')
)

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
    logger.info('in task_update_metrics with {} records and db {}'.format(len(metrics_records), metrics_rds))
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
    global metrics_rds
    logger.info('in init_worker, creating database')
    metrics_rds = Metrics('', {'METRICS_DATABASE': 'postgresql://foo/metrics'})


@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    global metrics_rds
    if metrics_rds:
        logger.info('in shutdown_worker, should we close database?')

        
def queue_metrics(bibcodes_filename):
    """read list of changed bibcodes from file, get full records from database and queue them for delivery
    full records come from the localhost Postgres database
    """
    logger.info('queuing bibcodes from file {}'.format(bibcodes_filename))
    metrics_db =  Metrics('metrics', {'METRICS_DATABASE': 'postgresql://postgres@localhost:5432/postgres'})
    metrics_buffer = []
    f = open(bibcodes_filename)
    if f is None:
        logger.error('could not open {}'.format(bibcodes_filename))
        return
    while True:
        line = f.readline()
        if len(line) == 0:
            logger.info('final saving {} records'.format(len(metrics_buffer)))
            if len(metrics_buffer) > 0:
                task_update_metrics.delay(metrics_buffer)
            return
        bibcode = line.strip()
        metrics_current = metrics_db.read(bibcode)
        if metrics_current:
            d = dict(metrics_current.items())
            metrics_buffer.append(d)
            if len(metrics_buffer) >= 100:
                task_update_metrics.delay(metrics_buffer)
                metrics_buffer = []
        else:
            logger.info('bibcode has not metrics entry {}'.format(bibcode))



if __name__ == '__main__':
    app.start()

        
