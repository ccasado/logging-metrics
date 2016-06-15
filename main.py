#!/usr/bin/env python

from Config import Config
import requests
import statsd
import json
import argparse
import sys
import sched, time
from time import gmtime, strftime
import os

parser = argparse.ArgumentParser(description='Logging statistics using Statsd, Graphite e Graphana.')
parser.add_argument('--env', type=str, choices=['devqa', 'prod'], required=True,
                    help='Run the collect data on this environment')
args = parser.parse_args()

config = Config()
env = args.env
eshost = config.ConfigSectionMap(env)['eshost']
esport = config.ConfigSectionMap(env)['esport']
esclustername = config.ConfigSectionMap(env)['esclustername']
statsHost = config.ConfigSectionMap("Statsd")['statshost']
statsPort = config.ConfigSectionMap("Statsd")['statsport']
statsProject = config.ConfigSectionMap("Statsd")['statsproject']
stats = statsd.StatsClient(statsHost, statsPort)

def metricsByNodes():
	try:
		url = 'http://' + eshost + ':' + esport + '/_nodes/stats'
		r = requests.get(url, timeout=1.000)
	except requests.exceptions.RequestException as e:
		print e
		sys.exit(1)
	es_stats = json.loads(r.text)
	if esclustername == es_stats["cluster_name"]:
		metrics = {}
		for node in es_stats["nodes"].values():
			node_name = node["name"]
			metrics[node_name + '.os.cpu_percent'] = node["os"]["cpu_percent"]
			metrics[node_name + '.os.load_average'] = node["os"]["load_average"]
			metrics[node_name + '.os.mem.free_percent'] = node["os"]["mem"]["free_percent"]
			metrics[node_name + '.os.mem.used_percent'] = node["os"]["mem"]["used_percent"]
			metrics[node_name + '.os.swap.free_in_bytes'] = node["os"]["swap"]["free_in_bytes"]
			metrics[node_name + '.os.swap.used_in_bytes'] = node["os"]["swap"]["used_in_bytes"]
			metrics[node_name + '.jvm.mem.heap_used_percent'] = node["jvm"]["mem"]["heap_used_percent"]
		return metrics
	else:
		print "Cluster data not available. Check esclustername in your properties file."
		sys.exit(1)

def sendToStatsd(key, value):
	#print "%s Sending to statsd - %s:%s" % (strftime("%d %b %Y %H:%M:%S", gmtime()), key, value)
	stats.incr(key, value)

if __name__ == '__main__':
	for key, value in metricsByNodes().iteritems():
		print key, value

