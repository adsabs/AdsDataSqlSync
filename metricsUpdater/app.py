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

# run the workers with:
# root@adsqb:/app# celery -A metricsUpdater.app worker -l info --concurrency=5
# queue data from bibcode using queue_cli

logger = get_task_logger('AdsDataSqlSync')
conf = utils.load_config()

global metrics_rds_collection
global conf

# celery configuration mostly from ADSimportpipeline/aip/app.py
app = Celery('AdsDataSqlSync',
             broker=conf.get('CELERY_BROKER', 'pyamqp://'),
             include=['metricsUpdater.app'])

exch = Exchange(conf.get('CELERY_UPDATEMETRICS_EXCHANGE', 'update-metrics'), type=conf.get('CELERY_UPDATEMETRICS_EXCHANGE_TYPE', 'topic'))

app.conf.CELERY_QUEUES = (
    Queue('errors', exch, routing_key='errors', durable=False, message_ttl=24*3600*5),
    Queue('update-localhost-metrics', exch, routing_key='update-metrics.localhost.#'),
    Queue('update-test-metrics', exch, routing_key='update-metrics.test.#'),
    Queue('update-prod-metrics', exch, routing_key='update-metrics.prod.#'),
)

app.conf.update(conf)


class MyTask(Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error('{0!r} failed: {1!r}'.format(task_id, exc))




@app.task(base=MyTask, bind=True, exchange="update-metrics")
def task_update_metrics(self, metrics_records):
    """
    Depending on the routing key, the update metrics task will be executed using
    a specific destination database/schema
    """
    v_routing_key = self.request.delivery_info['routing_key'].split(".")

    if len(v_routing_key) != 3:
        logger.error("Wrong routing key: " + self.request.delivery_info['routing_key'])
        raise Ignore()

    action, database, schema = v_routing_key

    if database not in ("localhost", "prod", "test"):
        logger.error("Unknown database: " + database)
        raise Ignore()

    if schema not in ("tmp", "staging", "rds"):
        logger.error("Unknown schema: " + schema)
        raise Ignore()

    _task_update_metrics(metrics_records, database, schema)


def _task_update_metrics(metrics_records, database, schema):
    """
    Receives a set of full metrics records and updates rds based metrics Postgres instance

    Metrics record includes primary key index field, staging value will not match rds database
    """
    global metrics_rds_collection
    global conf
    try:
        metrics_rds = metrics_rds_collection[database][schema]
    except KeyError:
        logger.error("Data cannot be written to database '{}' schema '{}' because a connection was not established during worker initialization".format(database, schema))
        raise Ignore()

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
    Open database connections to all the possible databases defined in the
    configuration file.

    There will be just one connection per database+schema per worker, which is
    accessed using a global variable.

    Source: http://stackoverflow.com/questions/14526249/celery-worker-database-connection-pooling
    """
    # TODO: Adapt Metrics to support multiple schemas with one database connection
    global metrics_rds_collection
    global conf

    metrics_rds_collection = {}
    for database in conf['METRICS_CONNECTIONS'].keys():
        database_connection_string = conf['METRICS_CONNECTIONS'][database]
        metrics_rds_collection[database] = {}
        for schema in conf['METRICS_SCHEMA_NAMES'].keys():
            schema_name = conf['METRICS_SCHEMA_NAMES'][schema]
            try:
                metrics_rds_collection[database][schema] = Metrics(schema_name, {'METRICS_DATABASE': database_connection_string})
            except sqlalchemy.exc.OperationalError:
                logger.warn("Connection to database '{}' schema '{}' failed, this worker will not be able to modify this database".format(database, schema))
                break # Do not continue with other schemas in this database
            else:
                logger.info("Connection to database '{}' schema '{}' succeeded".format(database, schema))


@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    """
    Close all open database connections when the worker ends.
    """
    global metrics_rds_collection
    for database in metrics_rds_collection.keys():
        for schema in metrics_rds_collection[database]:
            logger.warn("Closing connection to database '{}' schema '{}'...".format(database, schema))
            metrics_rds_collection[database][schema].connection.close()


if __name__ == '__main__':
    app.start()


