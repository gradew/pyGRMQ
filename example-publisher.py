#!/usr/bin/env python
import pika, random, string, socket, time
from GRMQ import GRMQ

class GRMQClient(GRMQ):
    def log(self, msg):
        print("[DEBUG] %s" % msg)

    def on_message(self, unused_channel, basic_deliver, properties, body):
        pass

if __name__ == '__main__':
        g = GRMQClient('amqp://guest:guest@localhost:5672/%2F', 'BULMA_RPI_CMD', 'fanout', socket.gethostname() + '-' + ''.join(random.choice(string.lowercase) for i in range(4)), '', True)
        g.setPublisher()
        g.connect()
        g.publish_json({"message":"test"})

