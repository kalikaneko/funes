#!env/bin/python
"""
Funes client.

    funes.py --add URL
    funes.py --list

"""
import json
import sys
from optparse import OptionParser

from twisted.internet import reactor
from txzmq import ZmqEndpoint, ZmqFactory, ZmqREQConnection
import zmq

from config import ENDPOINT

parser = OptionParser("")
parser.add_option("-a", "--add", dest="do_add",
                  help="Option: add a document")
parser.add_option("-l", "--list", action="store_true", dest="do_list",
                  help="List all documents")
parser.set_defaults(method="connect", endpoint=ENDPOINT)

(options, args) = parser.parse_args()


def get_zmq_connection():
    zf = ZmqFactory()
    e = ZmqEndpoint(options.method, options.endpoint)
    return ZmqREQConnection(zf, e)


def usage():
    print """Usage: funes --add URL
       funes --list"""

if not options.do_list and options.do_add is None:
    usage()
    sys.exit(1)


def do_list(stuff):
    doc_list = json.loads(stuff[0])
    for num, doc in enumerate(doc_list):
        print ">> [{num:>2}]".format(num=num), doc['url']


def do_print(stuff):
    ok = json.loads(stuff[0])
    print ["ERROR", "ok"][int(bool(ok))]


def send_command():
    if options.do_add is not None:
        data = ("add", options.do_add)
        cb = do_print

    elif options.do_list:
        data = ("list",)
        cb = do_list

    s = get_zmq_connection()
    try:
        d = s.sendMsg(*data)
    except zmq.error.Again:
        print "[ERROR] Server is down :("
    d.addCallback(cb)
    d.addCallback(lambda x: reactor.stop())

if __name__ == "__main__":
    reactor.callWhenRunning(reactor.callLater, 0, send_command)
    reactor.run()
