from __future__ import absolute_import, unicode_literals
from celery import Celery
from celery.utils.log import get_task_logger
from celery import Task
from celery.signals import worker_process_init
from celery.signals import worker_process_shutdown
from celery import bootsteps
from celery.bin import Option
from celery import current_app
from celery.exceptions import Ignore
import sqlalchemy

from kombu import Exchange, Queue

import sys
sys.path.append("..")
import utils
from metrics import Metrics

# Code for updating the metrics rds-based Postgres instance
# This code is based on Celery usage in ADSimportpipeline/aip

# 1) Prepare rabbitmq:
#   docker exec -it backoffice_rabbitmq rabbitmqctl add_user admin password
#   docker exec -it backoffice_rabbitmq rabbitmqctl add_vhost metrics
#   docker exec -it backoffice_rabbitmq rabbitmqctl set_user_tags admin administrator
#   # Permissions to: Configure / Write / Read
#   docker exec -it backoffice_rabbitmq rabbitmqctl set_permissions -p metrics admin ".*" ".*" ".*"
#
# 2) run the workers with:
#   root@adsqb:/app# celery -A metricsUpdater.app worker -l info --concurrency=5 --vhost metrics
#
# queue data from bibcode using queue_cli

logger = get_task_logger('AdsDataSqlSync')
conf = utils.load_config()

global metrics_rds

# celery configuration mostly from ADSimportpipeline/aip/app.py
app = Celery('AdsDataSqlSync',
             broker=conf.get('CELERY_BROKER', 'pyamqp://'),
             include=['metricsUpdater.app'])

exch = Exchange(conf.get('CELERY_DEFAULT_EXCHANGE', 'update-metrics'), type=conf.get('CELERY_DEFAULT_EXCHANGE_TYPE', 'topic'))

app.conf.CELERY_QUEUES = (
    Queue('errors', exch, routing_key='errors', durable=False, message_ttl=24*3600*5),
    Queue('update-metrics', exch, routing_key='update-metrics'),
)

app.user_options['worker'].add(
    Option('--vhost', dest='vhost', default=None, type=str, nargs=1, help='RabbitMQ virtual host')
)

class CustomArgs(bootsteps.Step):
    def __init__(self, worker, vhost, **options):
        if vhost is not None and len(vhost) == 1:
            vhost = vhost[0]
        else:
            vhost = "metrics"

        app.conf.VHOST = vhost
        app.conf.BROKER_URL = "/".join(app.conf.BROKER_URL.split("/")[:-1] + [vhost,])
        logger.info("RabbitMQ vhost set to '{}'".format(vhost))

app.steps['worker'].add(CustomArgs)

app.conf.update(conf)


class MyTask(Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error('{0!r} failed: {1!r}'.format(task_id, exc))




@app.task(base=MyTask, bind=True, queue="update-metrics")
def task_update_metrics(self, metrics_records):
    """
    Receives a set of full metrics records and updates rds based metrics Postgres instance

    Metrics record includes primary key index field, staging value will not match rds database
    """
    #v_routing_key = self.request.delivery_info['routing_key']

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




@worker_process_init.connect
def init_worker(**kwargs):
    """
    Open a database connection to the one that matches the RabbitMQ vhost we
    are using.

    Source: http://stackoverflow.com/questions/14526249/celery-worker-database-connection-pooling
    """
    global metrics_rds

    vhost = app.conf.VHOST
    database_connection_string = app.conf.UPDATE_METRICS_VHOST[vhost]['db']
    schema_name = app.conf.UPDATE_METRICS_VHOST[vhost]['schema']
    logger.info("Openning connection to database..." + database_connection_string)
    metrics_rds = Metrics(schema_name, {'METRICS_DATABASE': database_connection_string})



@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    """
    Close all open database connections when the worker ends.
    """
    global metrics_rds
    logger.info("Closing connection to database...")
    metrics_rds.connection.close()


if __name__ == '__main__':
    app.start()


