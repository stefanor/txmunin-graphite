#!/usr/bin/env python

import cPickle as pickle
import ConfigParser
import re
import socket
import struct
import time

from twisted.application.service import Application, Service
from twisted.internet import defer, reactor, task
from twisted.internet.protocol import ClientCreator, ClientFactory, Protocol
from twisted.protocols.basic import LineReceiver
from twisted.python import log

CONFIG_FILE = 'config.ini'
DEBUG = False


class MuninClient(LineReceiver):
    delimiter = '\n'

    def connectionMade(self):
        log.msg("Connected to munin-node")
        # temporary storage for multi-line responses
        self.values = {}
        # expected responses: (name, *args)
        self.queue = [('hello',)]
        self.node_name = None

    def lineReceived(self, line):
        if DEBUG:
            log.msg("Line recived: %s" % line)
        if not self.queue:
            log.err('Unsolicited message: %s' % line)
        getattr(self, 'parse_%s' % self.queue[0][0])(line, *self.queue[0][1:])

    def parse_hello(self, line):
        self.queue.pop(0)
        m = re.match(r'^# munin node at (.+)$', line)
        if not m:
            log.err('Unexpected greeting: %s' % line)
            return
        self.node_name = m.group(1)

    def parse_list(self, line, d):
        self.queue.pop(0)
        services = line.split()
        if DEBUG:
            log.msg("Services: %r" % services)
        d.callback(services)

    def parse_fetch(self, line, d):
        if line == '.':
            self.queue.pop(0)
            values, self.values = self.values, {}
            if DEBUG:
                log.msg("Values: %r" % values)
            d.callback(values)
            return

        try:
            k, v = line.split()
        except ValueError:
            log.msg('Unparseable metric: %s' % line)
            return
        self.values[k] = (v, time.time())

    def do_list(self):
        d = defer.Deferred()
        self.queue.append(('list', d))
        self.sendLine('list')
        return d

    def do_fetch(self, service):
        d = defer.Deferred()
        self.queue.append(('fetch', d))
        self.sendLine('fetch %s' % service)
        return d

    def do_quit(self):
        self.queue.append(('quit',))
        self.sendLine('quit')

    def collect_metrics(self, services):
        "Fetch the metrics for all specified services"
        services = frozenset(services)
        metrics = {}
        d = defer.Deferred()

        def callback(value, service):
            metrics[service] = value
            if DEBUG:
                log.msg("%i of %i" % (len(metrics), len(services)))
            if frozenset(metrics.keys()) == services:
                d.callback(metrics)

        for service in services:
            self.do_fetch(service).addCallback(callback, service)
        return d


class CarbonClient(Protocol):
    def sendStats(self, stats):
        payload = pickle.dumps(stats, 2)
        header = struct.pack('!L', len(payload))
        self.transport.write(header + payload)


class TwistedMuninService(Service):
    name = 'twisted-munin-service'

    def __init__(self, munin_addr, carbon_addr, interval):
        self.munin_addr = munin_addr
        self.carbon_addr = carbon_addr
        self.interval = interval

        self.task = task.LoopingCall(self.collect)

    @defer.inlineCallbacks
    def collect(self):
        # Start connecting to carbon first, it's remote...
        carbon_c = ClientCreator(reactor, CarbonClient)
        carbon = carbon_c.connectTCP(*self.carbon_addr)

        munin_c = ClientCreator(reactor, MuninClient)
        try:
            munin = yield munin_c.connectTCP(*self.munin_addr)
        except Exception:
            log.err("Unable to connect to munin-node")
            return
        services = yield munin.do_list()
        stats = yield munin.collect_metrics(services)
        munin.do_quit()

        reverse_name = '.'.join(munin.node_name.split('.')[::-1])
        flattened = []
        for service, metrics in stats.iteritems():
            for metric, (value, timestamp) in metrics.iteritems():
                path = 'servers.%s.%s.%s' % (reverse_name, service, metric)
                flattened.append((path, (timestamp, value)))

        try:
            carbon = yield carbon
        except Exception:
            log.err("Unable to connect to carbon")
            return
        yield carbon.sendStats(flattened)
        carbon.transport.loseConnection()

    def startService(self):
        self.task.start(self.interval)
        Service.startService(self)

    def stopService(self):
        self.task.stop()
        Service.stopService(self)


def config_get(config, section, option, default):
    "Convenience function for accessing ConfigParser objects with a default"
    if config.has_option(section, option):
        return config.get(section, option)
    else:
        return default


def main(twistd=False):
    global application

    config = ConfigParser.ConfigParser()
    if not config.read(CONFIG_FILE):
        raise Exception("Could not open configuration file: %s" % CONFIG_FILE)

    host = config_get(config, 'munin', 'host', 'localhost')
    port = int(config_get(config, 'munin', 'port', '4949'))
    munin_addr = (host, port)

    host = config_get(config, 'carbon', 'host', 'localhost')
    port = int(config_get(config, 'carbon', 'port', '2004'))
    carbon_addr = (host, port)

    interval = int(config_get(config, 'general', 'interval', '60'))

    service = TwistedMuninService(munin_addr, carbon_addr, interval)
    if twistd:
        application = Application('twisted-to-munin')
        service.setServiceParent(application)
    else:
        service.startService()
        reactor.run()


if __name__ == '__main__':
    main()
elif __name__ == '__builtin__':
    main(twistd=True)
