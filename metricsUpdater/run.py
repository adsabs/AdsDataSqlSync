import argparse
import sys
import re
import app as app_module
sys.path.append("../")
from metrics import Metrics
import metricsUpdater.tasks
   
def main():
    parser = argparse.ArgumentParser(description='Read metrics records for and queue them for writing by workers')
    parser.add_argument('bibcodes_filename', type=str, help='File with bibcodes')
    args = parser.parse_args()
    queue_metrics(args.bibcodes_filename)

    
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
    bibcodes = []
    final_flag = False
    f = open(bibcodes_filename)
    if f is None:
        logger.error('could not open {}'.format(bibcodes_filename))
        return
    while True:
        line = f.readline()
        if len(line) == 0:
            final_flag = True
        else:
            bibcode = line.strip()
            bibcodes.append(bibcode)

        if len(bibcodes) >= 100 or final_flag:
            logger.info('time to queue metrics data for {} bibcodes'.format(len(bibcodes)))
            if len(bibcodes) > 0:
                db_records = metrics_db.get_by_bibcodes(bibcodes)
                update_buffer = []
                for db_record in db_records:
                    update_buffer.append(app_module.MetricsUpdaterCelery.cleanup(db_record))
                metricsUpdater.tasks.task_update_metrics.delay(update_buffer)
            logger.info('added {} metrics records to queue, out of {}'.format(len(update_buffer), len(bibcodes)))
            bibcodes = []

        if final_flag:
            metrics_db.connection.close()
            return


    
if __name__ == "__main__":
    main()
