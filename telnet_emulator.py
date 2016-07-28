import gevent
from gevent.server import StreamServer
from gevent.socket import error
from random import sample
import sys

def handle(socket, address):
    print 'New connection: %s' % str(address)

    def periodic_send(socket):
        while True:
            s = 'CARD|%s\r\n' % sample(xrange(10000000000), 1)
            socket.send(s)
            gevent.sleep(0)

    def read():
        fileobj = socket.makefile(mode='rb')
        while True:
            line = fileobj.readline()
            if not line:
                print 'Client disconnect'
                break
            print 'Got line: %s' % line
        fileobj.close()

    gevent.spawn(periodic_send, socket)
    read()

server = StreamServer(('127.0.0.1', int(sys.argv[1])), handle)
server.serve_forever()