import argparse
import sys
import re
import app as app_module
sys.path.append("../")
from metrics import Metrics
import metricsUpdater.tasks
from utils import queue_rows

   
def main():
    parser = argparse.ArgumentParser(description='Read metrics records for and queue them for writing by workers')
    parser.add_argument('bibcodes_filename', type=str, help='File with bibcodes')
    parser.add_argument('changed', type=bool, help='File with bibcodes')
    args = parser.parse_args()
    if args.changed:
        pass
    else:
        queue_metrics(args.bibcodes_filename)


def queue_changed_metrics(schema='metrics'):
    nonbib = SqlSync('nonbib')
    delta_db = nonbib.create_delta_rows('metricsstaging')
    db_connection = app_obj.conf['METRICS_READ_CONNECTION']
    metrics_db =  Metrics(db_schema, {'METRICS_DATABASE': db_connection})
    queue_changed_rows(delta_db, metrics_db, metricsUpdater.tasks.task_update_metrics, logger)
    nonbib.connection.close()
    metrics_db.connection.close()
    
    
def queue_metrics(bibcodes_filename): 
    """
    Read list of changed bibcodes, get full records from database and queue them for delivery using the a RabbitMQ vhost 
    
    The workers listening will save to the correct destination database/schema.
    Full records come from the localhost Postgres database.
    """
    app_obj = app_module.MetricsUpdaterCelery('metricsUpdater')
    logger = app_obj.logger
    db_connection = app_obj.conf['METRICS_READ_CONNECTION']
    db_schema = app_obj.conf['METRICS_READ_SCHEMA']
    celery_connection = app_obj.conf['CELERY_BROKER']
    logger.info('read bibcodes from file {}, queue records from database {}, schema {}, on queue {}'
        .format(bibcodes_filename, re.sub('//.*@', '', db_connection), db_schema, re.sub('//.*@', '', celery_connection)))
    metrics_db =  Metrics(db_schema, {'METRICS_DATABASE': db_connection})
    queue_rows(bibcodes_filename, metrics_db, metricsUpdater.tasks.task_update_metrics, logger)
    metrics_db.connection.close()

    
if __name__ == "__main__":
    main()
