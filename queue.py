
import pika, json
import logging

from sqlalchemy.sql import select

import utils
import row_view


class Queue():
    """send changed bibcodes to the solr update rabbitmq queue
    This code puts all changed bibcodes on the queue.  Perhaps it
    could check to see of any Solr-related data changed, but it does not.

    A single Postgres database holds all the bibcodes that hvae changed, 
    one bibcode per row.  It does not store which columns have changed.
    """

    def __init__(self, row_view_schema='ingest', passed_config=None):
        self.config = {}
        self.config.update(utils.load_config())
        if passed_config:
            self.config.update(passed_config)
        self.row_view_schema = row_view_schema
        self.logger = logging.getLogger('AdsDataSqlSync')


    def add_changed_bibcodes(self):
        """put changed bibcodes in solr update queue"""
        sql_sync = SqlSync(self.row_view_schema)
        delta_table = sql_sync.get_delta_table()
        s = select([delta_table])
        rp = sql_sync.sql_sync_connection.execute(s)
        count = 0
        max_payload_size = 100
        payload = []
        for record in rp:
            payload.append(record['bibcode'])
            if len(payload) >= max_payload_size:
                self.publish_to_rabbitmq(payload)
                payload = []
            count += 1
            if count % 100000 == 0:
                self.logger.debug('saving changed bibcodes to queue, count = {}'.format(count))
        if len(payload) > 0:
            self.publish_to_rabbitmq(payload)
        self.logger.info('{} changed bibcodes set to queue'.format(count))


    def publish_to_rabbitmq(self, payload):
        """write the passed payload to the rabbitmq specified by config file values"""
        url = self.config.get('RABBITMQ_URL', 'amqp://admin:password@localhost:5672/ADSimportpipeline')
        exchange = self.config.get('RABBITMQ_EXCHANGE', 'MergerPipelineExchange')
        route = self.config.get('RABBITMQ_ROUTE', 'SolrUpdateRoute')
        connection = pika.BlockingConnection(pika.URLParameters(url))
        channel = connection.channel()
        channel.basic_publish('', route, json.dumps(payload))
        connection.close()

    def init_rabbitmq(self):
        """useful for testing to create exchanges and routes on new rabbitmq instance"""
        url = self.config.get('RABBITMQ_URL', 'amqp://admin:password@localhost:5672/ADSimportpipeline')
        connection = pika.BlockingConnection(pika.URLParameters(url))
        channel = connection.channel()
        exchange = self.config.get('RABBITMQ_EXCHANGE', 'MergerPipelineExchange')
        route = queue=self.config.get('RABBITMQ_ROUTE', 'SolrUpdateRoute')
        channel.exchange_declare(exchange=exchange)
        channel.queue_declare(queue=route)
        channel.basic_publish(exchange='', routing_key=queue,
                              body='hello, world')
        connection.close()
        print '!!'
