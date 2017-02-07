#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pika, json

class GRMQ(object):
    EXCHANGE = ''
    EXCHANGE_TYPE = ''
    QUEUE = ''
    ROUTING_KEY = ''
    EXCLUSIVE_QUEUE = True
    IS_PUBLISHER = False
    MYNAME = ''

    def __init__(self, amqp_url, exchange, exchange_type, queue_name, routing_key, exclusive_queue):
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url

        self.EXCHANGE=exchange
        self.EXCHANGE_TYPE=exchange_type
        self.QUEUE=queue_name
        self.ROUTING_KEY=routing_key
        self.EXCLUSIVE_QUEUE=exclusive_queue

        self.MYNAME=self.QUEUE

    def setPublisher(self):
        self.IS_PUBLISHER=True

    def log(self, msg):
        pass

    def connect(self):
        self.log('Connecting to %s' % (self._url,))
        if self.IS_PUBLISHER==False:
            return pika.SelectConnection(pika.URLParameters(self._url),
                                         self.on_connection_open,
                                         stop_ioloop_on_close=False)
        else:
            self._connection=pika.BlockingConnection(pika.URLParameters(self._url))
            self._channel=self._connection.channel()
            self._channel.exchange_declare(exchange=self.EXCHANGE, type=self.EXCHANGE_TYPE)

    def on_connection_open(self, unused_connection):
        self.log('Connection opened')
        self._connection.add_on_close_callback(self.on_connection_closed)
        self.open_channel()

    def on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self.log('Connection closed, reopening in 5 seconds: (%s) %s' % (reply_code, reply_text))
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        self._connection.ioloop.stop()

        if not self._closing:
            self._connection = self.connect()
            self._connection.ioloop.start()

    def open_channel(self):
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        self.setup_exchange(self.EXCHANGE)

    def on_channel_closed(self, channel, reply_code, reply_text):
        self.log('Channel %i was closed: (%s) %s' % (channel, reply_code, reply_text))
        self._connection.close()

    def setup_exchange(self, exchange_name):
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.EXCHANGE_TYPE)

    def on_exchange_declareok(self, unused_frame):
        self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name):
        self._channel.queue_declare(self.on_queue_declareok, queue_name, exclusive=self.EXCLUSIVE_QUEUE)

    def on_queue_declareok(self, method_frame):
        self._channel.queue_bind(self.on_bindok, self.QUEUE,
                                 self.EXCHANGE, self.ROUTING_KEY)

    def on_bindok(self, unused_frame):
        self.start_consuming()

    def start_consuming(self):
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self._consumer_tag = self._channel.basic_consume(self.on_message, self.QUEUE)

    def on_consumer_cancelled(self, method_frame):
        if self._channel:
            self._channel.close()

    def publish_json(self, message):
        properties = pika.BasicProperties(app_id=self.MYNAME, content_type='application/json', headers=message)
        self._channel.basic_publish(self.EXCHANGE, '', json.dumps(message, ensure_ascii=False), properties)

    def template_on_message(self, unused_channel, basic_deliver, properties, body):
        print(" **** Received message # %s from %s: %s" % (basic_deliver.delivery_tag, properties.app_id, body))
        self.acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        if self._channel:
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_cancelok(self, unused_frame):
        self.close_channel()

    def close_channel(self):
        self._channel.close()

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()

    def close_connection(self):
        self._connection.close()

