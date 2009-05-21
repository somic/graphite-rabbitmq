#!/usr/bin/env python

"""

Parses Linux /proc/net/dev to get RX and TX bytes on interface IFACE

"""

import sys, os, time
sys.path += [ os.path.dirname(os.path.dirname(os.path.abspath(__file__))) ]
from graphite_rabbitmq_publish import GraphiteRabbitMQPublisher

try:
    iface = sys.argv[1]
    metric_prefix = sys.argv[2]
except:
    print "Usage: %s iface metric_prefix" % sys.argv[0]
    sys.exit(1)

def parse_proc_net_dev(iface):
    f = open('/proc/net/dev')
    r = 0
    t = 0
    for l in f:
        if l.find("%s:" % iface) == -1: continue
        spl = l.split()
        r, t = int(spl[0].split(':')[1]), int(spl[8])
    f.close()
    return r, t

rx = 0
tx = 0
first_sample = True
while True:
    try:
        pub = GraphiteRabbitMQPublisher()
        while True:
            new_rx, new_tx = parse_proc_net_dev(iface)
            if not first_sample:
                pub.publish([
                    "%s.rx %d" % (metric_prefix, new_rx-rx),
                    "%s.tx %d" % (metric_prefix, new_tx-tx)
                ])
            print "%s rx:%d tx:%d" % (time.asctime(), new_rx-rx, new_tx-tx)
            rx, tx = new_rx, new_tx
            first_sample = False
            time.sleep(60)
    except Exception, e:
        print e
        time.sleep(20)

