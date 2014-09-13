#!env/bin/python
"""
Funes server.
   funesd.py

"""
import json

from twisted.internet import defer, reactor
from txzmq import ZmqEndpoint, ZmqFactory, ZmqREPConnection
import txmongo

import config


def get_serializable_dict(d):
    return {k: v for k, v in d.items() if not k.startswith("_")}


def get_serializable_list(l):
    return [get_serializable_dict(d) for d in l]


class FunesZmqREPConnection(ZmqREPConnection):

    @defer.inlineCallbacks
    def start_db(self):
        mongo = yield txmongo.MongoConnection()
        db = getattr(mongo, config.DATABASE)
        self.docs = yield getattr(db, config.DOC_COLLECTION)

    def gotMessage(self, msgId, *parts):
        cmd = parts[0]
        if cmd == "shutdown":
            self.do_bye()
        elif cmd == "list":
            self.list_and_send_docs(msgId)
        else:
            self.add_doc(msgId, parts[1])

    @defer.inlineCallbacks
    def list_and_send_docs(self, msgId):
        docs = yield self.docs.find()
        docs_to_send = get_serializable_list(docs)
        reactor.callLater(0, self.reply, msgId, json.dumps(docs_to_send))

    @defer.inlineCallbacks
    def add_doc(self, msgId, url):
        doc_id = yield self.docs.insert({"url": url}, safe=True)
        ok = bool(doc_id)
        reactor.callLater(0, self.reply, msgId, json.dumps(ok))

    def do_greet(self):
        print "[+] Funes server running..."

    def do_bye(self):
        print "[+] Funes server stopped. Have a nice day."
        reactor.stop()


def get_zmq_connection():
    zf = ZmqFactory()
    e = ZmqEndpoint("bind", config.ENDPOINT)
    return FunesZmqREPConnection(zf, e)


if __name__ == "__main__":
    s = get_zmq_connection()
    reactor.callWhenRunning(s.start_db)
    reactor.callWhenRunning(s.do_greet)
    reactor.run()

__all__ = [FunesZmqREPConnection]
