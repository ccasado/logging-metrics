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
project = config.ConfigSectionMap(env)['project'] 
eshost = config.ConfigSectionMap(env)['eshost']
esport = config.ConfigSectionMap(env)['esport']
esclustername = config.ConfigSectionMap(env)['esclustername']
statsHost = config.ConfigSectionMap("Statsd")['statshost']
statsPort = config.ConfigSectionMap("Statsd")['statsport']
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
			first_key = project + '.' + env + '.' + esclustername + '.' + node_name 
			metrics[first_key + '.os.cpu_percent'] = node["os"]["cpu_percent"]
			metrics[first_key + '.os.load_average'] = node["os"]["load_average"]
			metrics[first_key + '.os.mem.free_percent'] = node["os"]["mem"]["free_percent"]
			metrics[first_key + '.os.mem.used_percent'] = node["os"]["mem"]["used_percent"]
			#metrics[first_key + '.os.swap.free_in_bytes'] = node["os"]["swap"]["free_in_bytes"]
			metrics[first_key + '.os.swap.used_in_bytes'] = node["os"]["swap"]["used_in_bytes"]
			metrics[first_key + '.jvm.mem.heap_used_percent'] = node["jvm"]["mem"]["heap_used_percent"]
			metrics[first_key + '.indices.indexing.index_current'] = node["indices"]["indexing"]["index_current"]
			metrics[first_key + '.indices.search.query_current'] = node["indices"]["search"]["query_current"]
			metrics[first_key + '.indices.docs.count'] = node["indices"]["docs"]["count"]
			#metrics[first_key + '.indices.docs.deleted'] = node["indices"]["docs"]["deleted"]
			metrics[first_key + '.indices.segments.count'] = node["indices"]["segments"]["count"]
			metrics[first_key + '.fs.total.total_in_bytes'] = node["fs"]["total"]["total_in_bytes"]
			metrics[first_key + '.fs.total.free_in_bytes'] = node["fs"]["total"]["free_in_bytes"]
			metrics[first_key + '.jvm.gc.collectors.young.collection_time_in_millis'] = node["jvm"]["gc"]["collectors"]["young"]["collection_time_in_millis"]
			metrics[first_key + '.jvm.gc.collectors.old.collection_time_in_millis'] = node["jvm"]["gc"]["collectors"]["old"]["collection_time_in_millis"]
		return metrics
	else:
		print "Cluster data not available. Check esclustername in your properties file."
		sys.exit(1)

def sendToStatsd(key, value):
	stats.incr(key, value)
	#print "%s Sending to statsd - %s:%s" % (strftime("%d %b %Y %H:%M:%S", gmtime()), key, value)

if __name__ == '__main__':
	for key, value in metricsByNodes().iteritems():
		sendToStatsd(key, value)

