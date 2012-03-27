from socket import *
from gamechanger.core.environment import environment

# Sends statistics to the stats daemon over UDP

statsd_host = 'localhost'
statsd_port = 8125

env = environment()
machine_id = gethostname().replace('.gamechanger.io', '').replace('.', '')

def timing(stat, time, sample_rate=1):
    """
    Log timing information
    >>> from python_example import Statsd
    >>> Statsd.timing('some.time', 500)
    """
    stats = {}
    stats[stat] = "%d|ms" % time
    send(stats, sample_rate)

def increment(stats, sample_rate=1):
    """
    Increments one or more stats counters
    >>> Statsd.increment('some.int')
    >>> Statsd.increment('some.int',0.5)
    """
    update_stats(stats, 1, sample_rate)

def decrement(stats, sample_rate=1):
    """
    Decrements one or more stats counters
    >>> Statsd.decrement('some.int')
    """
    update_stats(stats, -1, sample_rate)

def update_stats(stats, delta=1, sampleRate=1):
    """
    Updates one or more stats counters by arbitrary amounts
    >>> Statsd.update_stats('some.int',10)
    """
    if (type(stats) is not list):
        stats = [stats]
    data = {}
    for stat in stats:
        data[stat] = "%s|c" % delta

    send(data, sampleRate)

def set_gauge(stats, value):
    if (type(stats) is not list):
        stats = [stats]
    data = {}
    for stat in stats:
        data[stat] = "{0}|g".format(value)

    send(data)

def send(data, sample_rate=1):
    """
    Squirt the metrics over UDP
    """
    host = statsd_host
    port = statsd_port
    addr=(host, port)

    sampled_data = {}

    if(sample_rate < 1):
        import random
        if random.random() <= sample_rate:
            for stat in data.keys():
                value = sampled_data[stat]
                sampled_data[stat] = "%s|@%s" %(value, sample_rate)
    else:
        sampled_data=data

    udp_sock = None
    try:
        udp_sock = socket(AF_INET, SOCK_DGRAM)
        for stat in sampled_data.keys():
            value = data[stat]
            send_data = "%s.%s.%s:%s" % (env, stat, machine_id, value)
            udp_sock.sendto(send_data, addr)
    except:
        import sys
        from pprint import pprint
        print >> sys.stderr, "Unexpected error:", pprint(sys.exc_info())
        pass # we don't care
    finally:
        if udp_sock:
            udp_sock.close()

class Reporter(object):
    def __init__(self, prefix):
        self.prefix = prefix

    def qual_stat(self, stat):
        if type(stat) is list:
            return [self.qual_stat(s) for s in stat]
        return "{0}.{1}".format(self.prefix, stat)

    def timing(self, stat, time, sample_rate=1):
        timing(self.qual_stat(stat), time, sample_rate)

    def increment(self, stats, sample_rate=1):
        increment(self.qual_stat(stats), sample_rate)

    def decrement(self, stats, sample_rate=1):
        decrement(self.qual_stat(stats), sample_rate)

    def set_gauge(self, stats, value):
        set_gauge(self.qual_stat(stats), value)