
import pika, json
import logging

from urlparse import urlparse
import requests
from sqlalchemy.sql import select

import utils
import row_view


class Queue():
    """send changed bibcodes to the solr update rabbitmq queue
    This code puts all changed bibcodes on the queue.  Perhaps it
    could check to see of any Solr-related data changed, but it does not.

    A single Postgres database holds all the bibcodes that have changed, 
    one bibcode per row.  It does not store which columns have changed.
    """

    def __init__(self, row_view_schema='ingest', passed_config=None):
        self.config = {}
        self.config.update(utils.load_config())
        if passed_config:
            self.config.update(passed_config)
        self.row_view_schema = row_view_schema
        self.logger = logging.getLogger('AdsDataSqlSync')

    def add_all_bibcodes(self):
        """put changed bibcodes in solr update queue"""
        sql_sync = row_view.SqlSync(self.row_view_schema)
        canonical_table = sql_sync.get_canonical_table()
        self.add_bibcodes_aux(canonical_table)
        
    def add_changed_bibcodes(self):
        """put changed bibcodes in solr update queue"""
        sql_sync = row_view.SqlSync(self.row_view_schema)
        delta_table = sql_sync.get_delta_table()
        self.add_bibcodes_aux(delta_table)

    def add_bibcodes_aux(self, t):
        """expects passed table has a column named bibcode"""
        s = select([t])
        sql_sync = row_view.SqlSync(self.row_view_schema)
        rp = sql_sync.sql_sync_connection.execute(s)
        count = 0
        max_payload_size = 100
        max_rows = self.config['MAX_ROWS']
        payload = []
        for record in rp:
            payload.append(record['bibcode'])
            if len(payload) >= max_payload_size:
                self.publish_to_rabbitmq(payload)
                payload = []
                self.logger.info('saving changed bibcodes to queue, count = {}'.format(count))
                self.logger.debug('bibcodes: {}'.format(str(payload)))
            count += 1
            if max_rows > 0 and count * max_payload_size > max_rows:
              break
        if len(payload) > 0:
            # here if we still have a part of a payload to save
            self.publish_to_rabbitmq(payload)
	    self.logger.info('saving final changed bibcodes to queue, count = {}'.format(count))
            self.logger.debug('bibcodes: {}'.format(str(payload)))
        self.logger.info('{} changed bibcodes sent to queue'.format(count))


    def publish_to_rabbitmq(self, payload):
        """write the passed payload to the rabbitmq specified by config file values"""
        url = self.config.get('RABBITMQ_URL', 'amqp://admin:password@localhost:5672/ADSimportpipeline')
        exchange = self.config.get('RABBITMQ_EXCHANGE', 'MergerPipelineExchange')
        route = self.config.get('RABBITMQ_ROUTE', 'SolrUpdateRoute')
	self.logger.debug('saving to rabbitmq at url {}'.format(url))
        connection = pika.BlockingConnection(pika.URLParameters(url))
        channel = connection.channel()
        channel.basic_publish('', route, json.dumps(payload))
        connection.close()

    def init_rabbitmq(self):
        """useful for testing to create exchanges and routes on new rabbitmq instance
        this code is currently only run from the command line via init-queue
        """
        #url = self.config.get('RABBITMQ_URL', 'amqp://admin:password@localhost:5672/ADSimportpipeline')
        url = self.config.get('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672/ADSimportpipeline')
        self.verify_vhost(url)
        connection = pika.BlockingConnection(pika.URLParameters(url))
        channel = connection.channel()
        exchange = self.config.get('RABBITMQ_EXCHANGE', 'MergerPipelineExchange')
        route = queue=self.config.get('RABBITMQ_ROUTE', 'SolrUpdateRoute')
        channel.exchange_declare(exchange=exchange)
        channel.queue_declare(queue=route)
        #channel.basic_publish(exchange='', routing_key=queue,
        #                      body='hello, world')
        connection.close()
        print 'queue init complete'
    

    def verify_vhost(self, url):
        """if the vhost does not exist, create it                                                                   
        pika does not appear to support this so we use the rest api
        vhost name should be only thing in the path section of the url
        note: we may need to also add a user to the queue
        """
        print 'received', url
        parsed = urlparse(url)
        # generate url for rest request vhost info, rest request must use http
        vhost_name = parsed.path
        u = 'http://' + parsed.netloc + '/api/vhosts' + vhost_name
        r = requests.get(u)
        if r.text[0] == '{':
            # vhost exists and json response holds details
            self.logger.info('{} vhost exists: {}'.format(vhost_name, r.text))
            print 'vhost already exists'
        elif '404 Not Found' in r.text:
            # here with html response, we need to create the vhost
            # send same url with a put to create
            self.logger.info('{} vhost does not exist, trying to create'.format(vhost_name))
            headers = {'Content-type': 'application/json'}
            r = requests.put(u, headers=headers)
            self.logger.info('{} created response = {} {}'.format(vhost_name, r, r.text))
            print 'created vhost {}, {}'.format(vhost_name, r.text)
        else:
            self.logger.error('in verify_vhost, could not parse RabbitMQ reponse to vhost check: {}'.format(r))
            print 'did not understand response', r
            print r.text


