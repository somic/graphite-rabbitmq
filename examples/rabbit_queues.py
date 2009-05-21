#!/usr/bin/env python
#
# graphite publisher for some basic info on rabbitmq queues
#
# requires py-interface:
# http://www.lysator.liu.se/~tab/erlang/py_interface/
#

COOKIE = 'PQDZQNHHATKKMBYYBHSS'
METRIC_PREFIX = 'rabbitmq.homeserver.queues'
RABBIT_NODE = 'rabbit@home'

metrics = [ "messages_ready", "messages_unacknowledged",
    "messages_uncommitted", "messages", "consumers", "memory" ]

import sys, os, time
sys.path += [ os.path.dirname(os.path.dirname(os.path.abspath(__file__))) ]
from graphite_rabbitmq_publish import GraphiteRabbitMQPublisher

from py_interface import erl_node, erl_eventhandler
from py_interface.erl_opts import ErlNodeOpts
from py_interface.erl_term import ErlAtom, ErlBinary

#from py_interface import erl_common
#erl_common.DebugOnAll()

rpc_args = [ ErlAtom('name') ] + [ ErlAtom(x) for x in metrics ]

def __msg_handler_list_queues(msg):
    data_lines = [ ]
    for q in msg:
        name = q[0][1][-1].contents
        for atoms_tuple in q[1:]:
                data_lines.append("%s.%s.%s %d" % \
                    (METRIC_PREFIX, name, atoms_tuple[0].atomText,
                    atoms_tuple[1]))
    print data_lines
    pub.publish(data_lines)
    erl_eventhandler.GetEventHandler().AddTimerEvent(60,
        rpc_list_queues, mbox=mbox)

def start_pyrabbitmqctl_node():
    node = erl_node.ErlNode("pyrabbitmqctl%d" % os.getpid(),
                            ErlNodeOpts(cookie=COOKIE))
    mbox = node.CreateMBox()
    return node, mbox

def rpc_list_queues(mbox, vhost="/"):
    mbox.SendRPC(
        ErlAtom(RABBIT_NODE),
        ErlAtom('rabbit_amqqueue'),
        ErlAtom('info_all'),
        [ ErlBinary(vhost), rpc_args ],
        __msg_handler_list_queues
    )

global pub
pub = GraphiteRabbitMQPublisher()

node, mbox = start_pyrabbitmqctl_node()
rpc_list_queues(mbox)
erl_eventhandler.GetEventHandler().Loop()

