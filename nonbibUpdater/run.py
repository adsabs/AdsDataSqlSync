import argparse
import sys
import re
import app as app_module
sys.path.append("../")
from row_view import SqlSync
import nonbibUpdater.tasks
   
def main():
    parser = argparse.ArgumentParser(description='Read nonbib records for and queue them for writing by workers')
    parser.add_argument('bibcodes_filename', type=str, help='File with bibcodes')
    args = parser.parse_args()
    queue_nonbib(args.bibcodes_filename)

    
def queue_nonbib(bibcodes_filename): 
    """
    Read list of changed bibcodes, get full records from database and queue them for delivery using the a RabbitMQ vhost 
    
    The workers listening will save to the correct destination database/schema.
    Full records come from the localhost Postgres database.
    """
    app_obj = app_module.NonbibUpdaterCelery('nonbib-updater')
    logger = app_obj.logger
    db_connection = app_obj.conf['NONBIB_READ_CONNECTION']
    db_schema = app_obj.conf['NONBIB_READ_SCHEMA']
    celery_connection = app_obj.conf['CELERY_BROKER']
    logger.info('read bibcodes from file {}, queue records from database {}, schema {}, on queue {}'
        .format(bibcodes_filename, re.sub('//.*@', '', db_connection), db_schema, re.sub('//.*@', '', celery_connection)))

    db_connection =  SqlSync(db_schema, {'INGEST_DATABASE': db_connection})
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
            logger.info('queue nonbib data for {} bibcodes'.format(len(bibcodes)))
            if len(bibcodes) > 0:
                db_records = db_connection.get_by_bibcodes(bibcodes)
                update_buffer = []
                for db_record in db_records:
                    update_buffer.append(app_module.NonbibUpdaterCelery.cleanup(db_record))
                nonbibUpdater.tasks.task_update_nonbib.delay(update_buffer)
            logger.info('added {} nonbib records to queue, out of {}'.format(len(update_buffer), len(bibcodes)))
            bibcodes = []

        if final_flag:
            db_connection.sql_sync_connection.close()
            return


    
if __name__ == "__main__":
    main()
