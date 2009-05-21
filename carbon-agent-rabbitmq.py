#!/usr/bin/env python
"""Copyright 2008 Orbitz WorldWide
   Copyright 2009 Dmitriy Samovskiy

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import sys
if sys.version_info[0] != 2 or sys.version_info[1] < 4:
  print 'Python version >= 2.4 and < 3.0 is required'
  sys.exit(1)

try:
  import graphite
except:
  print "Failed to import the graphite package. Please verify that this package"
  print "was properly installed and that your PYTHONPATH environment variable"
  print "includes the directory in which it is installed."
  print "\nFor example, you may need to run the following command:\n"
  print "export PYTHONPATH=\"/home/myusername/lib/python/:$PYTHONPATH\"\n"
  sys.exit(1)

import os, socket, time, traceback
from getopt import getopt
from signal import signal, SIGTERM
from subprocess import *
from select import select
from schemalib import loadStorageSchemas
from utils import daemonize, dropprivs, logify
import amqplib.client_0_8 as amqp
from graphite_rabbitmq_config import RABBITMQ_BROKER_DATA, GRAPHITE_QUEUE

debug = False
user = 'apache'

try:
  (opts,args) = getopt(sys.argv[1:],"du:h")
  assert ('-h','') not in opts
except:
  print """Usage: %s [options]

Options:
    -d          Debug mode
    -u user     Drop privileges to run as user
    -h          Display this help message
""" % os.path.basename(sys.argv[0])
  sys.exit(1)

for opt,val in opts:
  if opt == '-d':
    debug = True
  elif opt == '-u':
    user = val

if debug:
  logify()
else:
  daemonize()
  logify('log/agent.log')
  pf = open('pid/agent.pid','w')
  pf.write( str(os.getpid()) )
  pf.close()
  try: dropprivs(user)
  except: pass
print 'carbon-agent started (pid=%d)' % os.getpid()

def handleDeath(signum,frame):
  print 'Received SIGTERM, killing children'
  try:
    os.kill( persisterProcess.pid, SIGTERM )
    print 'Sent SIGTERM to carbon-persister'
    os.wait()
    print 'wait() complete, exitting'
  except OSError:
    print 'carbon-persister appears to already be dead'
  sys.exit(0)

signal(SIGTERM,handleDeath)

devnullr = open('/dev/null','r')
devnullw = open('/dev/null','w')

persisterPipe = map( str, os.pipe() )
print 'created persister pipe, fds=%s' % str(persisterPipe)

args = ['./carbon-persister.py',persisterPipe[0]]
persisterProcess = Popen(args,stdin=devnullr,stdout=devnullw,stderr=devnullw)
print 'carbon-persister started with pid %d' % persisterProcess.pid
pf = open('pid/persister.pid','w')
pf.write( str(persisterProcess.pid) )
pf.close()

writeFD = int(persisterPipe[1])

def write_to_persister(msg):
  data = "%s" % msg.body
  if not data.endswith("\n"): data += "\n"
  written = os.write(writeFD, data)
  msg.channel.basic_ack(msg.delivery_tag)
  assert written == len(data), "write_to_persister: wrote only %d of %d" \
                               % (written, len(data))

while True:
  try:
    conn = amqp.Connection(**RABBITMQ_BROKER_DATA)
    ch = conn.channel()
    ch.basic_consume(GRAPHITE_QUEUE, callback=write_to_persister)
    while ch.callbacks: ch.wait()
  except Exception, e:
    print '%s Got exception in loop: %s' % (time.asctime(), str(e))
    try: conn.close()
    except: pass
    time.sleep(1)


