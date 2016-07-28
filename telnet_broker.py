import gevent
from gevent.pool import Group
from gevent.coros import BoundedSemaphore
from gevent.monkey import patch_all; patch_all()
import logging

import redis
from redis.exceptions import ConnectionError

import socket
import sys
from telnetlib import Telnet
import time

from conf import *

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S')

pool = Group()

sem = BoundedSemaphore(2)

class RedisConnection(object):
    connection = None
    published = None
    host = None
    port = None

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.connect()


    def connect(self):
        while True:
            try:
                self.connection = redis.StrictRedis(self.host, self.port, db=0)
                self.publisher = self.connection.pubsub()
                self.publisher.subscribe('barrier')
                break
            except ConnectionError as e:
                logging.error('Cannot connect to Redis: %s' % e)
                gevent.sleep(REDIS_CONNECT_TIMEOUT)

        handle = gevent.spawn(self.handle)
        pool.add(handle)


    def handle(self):
        while True:
            try:
                msg = self.publisher.get_message()
            except ConnectionError as e:
                # Connection refused. Try to reconnect
                pass

            # Pass empty messages
            if not msg or msg['type'] != 'message':
                gevent.sleep(0.001)
                continue

            logging.info('Redis message: %s' % msg)
            # Parse message data
            try:
                srv, cmd = msg['data'].split('|')
            except IndexError:
                logging.error('Bad command: %s' % msg)

            # Find telnet connection by ip
            t = telnet_connections.get(srv)
            if not t:
                logging.error('Cannot get telnet connection for %s' % srv)
                continue

            # Send command to telnet connection
            logging.info('Sending command to %s: %s' % (srv, cmd))
            t.send('%s' % cmd)



class TelnetConnection(object):
    connection = None
    message_count = 0

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.connection = Telnet()
        self.connect()


    def connect(self):
        while True:
            try:
                self.connection.open(self.host, self.port,
                                     timeout=TELNET_CONNECTION_TIMEOUT)
                logging.info('Telnet connected to %s:%s' % (
                                                        self.host, self.port))
                break
            except socket.error as e:
                logging.error('Telnet connect to %s:%s error: %s' % (
                                                    self.host, self.port, e))
                gevent.sleep(TELNET_RECONNECT_TIMEOUT)
                continue
        handle = gevent.spawn(self.handle)
        pool.add(handle)


    def handle(self):
        while True:
            try:
                line = self.connection.read_until('\r\n',
                                                  timeout=TELNET_READ_TIMEOUT)
                line = line.strip()
                logging.info('Got from %s:%s: %s (%s)' % (self.host, self.port, line, self.message_count))
                self.message_count += 1
                continue

            except EOFError as e:
                logging.error('Telnet read %s:%s error: %s' % (
                                                    self.host, self.port, e))
                self.connect()

            except Exception as e:
                if e.message == 'timed out':
                    pass

    def send(self, msg):
        try:
            self.connection.write('%s\r\n' % msg)
        except socket.error as e:
            logging.error('Cannot send to %s:%s: %s' % (
                                                    self.host, self.port, e))
            self.connection.close()
            self.connect()


telnet_connections = {}

if __name__ == '__main__':
    try:
        red_con = RedisConnection(REDIS_HOST, REDIS_PORT)
        for srv in TELNET_HOSTS:
            t = TelnetConnection(srv[0], srv[1])
            telnet_connections['%s:%s' % (srv[0], srv[1])] = t

        pool.join()

    except KeyboardInterrupt:
        logging.info('CTRL+C, exiting.')