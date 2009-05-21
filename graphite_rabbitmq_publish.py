#!/usr/bin/env python

import time
import sys
import amqplib.client_0_8 as amqp
from graphite_rabbitmq_config import RABBITMQ_BROKER_DATA, \
  GRAPHITE_EXCHANGE, GRAPHITE_ROUTING_KEY

class GraphiteRabbitMQPublisher:
  def __init__(self, rabbitmq_broker_data=RABBITMQ_BROKER_DATA,
                exchange=GRAPHITE_EXCHANGE,
                routing_key=GRAPHITE_ROUTING_KEY):
    self.rabbitmq_broker_data = rabbitmq_broker_data
    self.exchange = exchange
    self.routing_key=routing_key
    self.__channel = None

  def channel(self):
    if self.__channel is None:
        self.conn = amqp.Connection(**self.rabbitmq_broker_data)
        self.__channel = self.conn.channel()
    return self.__channel
              
  def publish(self, data, **defaults):
    """
    data can be a dict {metric:value, ...}
    data can be a list ["metric value", "value", "metric value timestamp", ...]

    defaults can include timestamp and metric

    """
    try: defaults['timestamp']
    except KeyError: defaults['timestamp'] = int(time.time())

    payload_lines = [ ]
    if type(data) == dict:
        for k in data:
            payload_lines.append("%s %s %d" % (k, str(data[k]), 
                defaults['timestamp']))
    elif type(data) == list:
        for k in data:
            parts = str(k).strip().split()
            m = None    # metric
            v = None    # value
            t = None    # timestamp
            if len(parts) == 1:
                m = defaults['metric']
                v = k
                t = defaults['timestamp']
            elif len(parts) == 2:
                m,v = parts[:2]
                t = defaults['timestamp']
            elif len(parts) == 3:
                m, v, t = parts
            else:
                raise ArgumentError, "bad line: %s" % k

            payload_lines.append("%s %s %d" % (m, v, t))
    elif type(data) == str:
        if not len(data.strip().split()) == 3:
            raise ArgumentError, "bad line %s" % data
        payload_lines.append(data)

    self.channel().basic_publish(
        amqp.Message("\n".join(payload_lines), delivery_mode=2),
        exchange=self.exchange, routing_key=self.routing_key)


if __name__ == '__main__':
    try:
        assert len(sys.argv[1:]) > 0, "Nothing to send"
        GraphiteRabbitMQPublisher().publish(sys.argv[1:])
    except Exception, e:
        print "Error: %s" % str(e)
        print "Usage: %s 'metric value timestamp' ..." % sys.argv[0]


