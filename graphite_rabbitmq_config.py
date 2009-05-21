#!/usr/bin/env python

"""

Defines common configuration data + tool to create graphite queue.

"""


RABBITMQ_BROKER_DATA = {
    'host': 'localhost:5672',
    'userid': 'guest',
    'password': 'guest'
}

GRAPHITE_EXCHANGE = 'amq.direct'
GRAPHITE_ROUTING_KEY = 'graphite'
GRAPHITE_QUEUE = 'graphite_data'

if __name__ == '__main__':
    import sys
    import amqplib.client_0_8 as amqp

    conn = amqp.Connection(**RABBITMQ_BROKER_DATA)
    ch = conn.channel()
    try:
        ch.queue_declare(queue=GRAPHITE_QUEUE, passive=True)
        print "Queue %s already exists." % GRAPHITE_QUEUE
    except:
        ch = conn.channel()
        ch.queue_declare(queue=GRAPHITE_QUEUE, durable=True, auto_delete=False)
        ch.queue_bind(GRAPHITE_QUEUE, GRAPHITE_EXCHANGE, GRAPHITE_ROUTING_KEY)
        ch.close()
        conn.close()
        print "Queue %s created." % GRAPHITE_QUEUE



