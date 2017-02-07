#!/usr/bin/env python
# -*- coding: utf-8 -*-

import socket
from threading import Thread
import random, string
from GRMQ import GRMQ

class GRMQClient(GRMQ):
    def log(self, msg):
        print("[DEBUG] %s" % msg)

    def on_message(self, unused_channel, basic_deliver, properties, body):
        print(" -*-* Received message # %s from %s: %s" % (basic_deliver.delivery_tag, properties.app_id, body))
        self.acknowledge_message(basic_deliver.delivery_tag)

if __name__ == '__main__':
    g = GRMQClient('amqp://guest:guest@localhost:5672/%2F', 'BULMA_RPI_CMD', 'fanout', socket.gethostname() + '-' + ''.join(random.choice(string.lowercase) for i in range(4)), '', True)
    try:
        g.run()
    except KeyboardInterrupt:
        g.stop()

