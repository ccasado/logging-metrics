#!/usr/bin/env python

import requests
import statsd
import json
import argparse
import sys
import sched, time
from time import gmtime, strftime
import os
import logging

def metricsByNodes():
	try:
		url = 'http://' + ESHOST + ':' + ESPORT + '/_nodes/stats'
		r = requests.get(url, timeout=1.000)
	except requests.exceptions.RequestException as e:
		logging.exception(e)
		sys.exit(1)
	es_stats = json.loads(r.text)
	if ESCLUSTERNAME == es_stats["cluster_name"]:
		metrics = {}
		for node in es_stats["nodes"].values():
			node_name = node["name"]
			if node_name in ESNODESNAME:
				first_key = PROJECT + '.' + ENV + '.' + ESCLUSTERNAME + '.' + node_name 
				metrics[first_key + '.os.cpu_percent'] = node["os"]["cpu_percent"]
				metrics[first_key + '.os.load_average'] = node["os"]["load_average"]
				metrics[first_key + '.os.mem.free_percent'] = node["os"]["mem"]["free_percent"]
				metrics[first_key + '.os.mem.used_percent'] = node["os"]["mem"]["used_percent"]
				metrics[first_key + '.os.swap.used_in_bytes'] = node["os"]["swap"]["used_in_bytes"]
				metrics[first_key + '.jvm.mem.heap_used_percent'] = node["jvm"]["mem"]["heap_used_percent"]
				metrics[first_key + '.indices.indexing.index_current'] = node["indices"]["indexing"]["index_current"]
				metrics[first_key + '.indices.indexing.index_time_in_millis'] = node["indices"]["indexing"]["index_time_in_millis"]
				metrics[first_key + '.indices.search.query_current'] = node["indices"]["search"]["query_current"]
				metrics[first_key + '.indices.search.query_time_in_millis'] = node["indices"]["search"]["query_time_in_millis"]
				metrics[first_key + '.indices.docs.count'] = node["indices"]["docs"]["count"]
				metrics[first_key + '.indices.segments.count'] = node["indices"]["segments"]["count"]
				metrics[first_key + '.jvm.gc.collectors.young.collection_count'] = node["jvm"]["gc"]["collectors"]["young"]["collection_count"]
				metrics[first_key + '.jvm.gc.collectors.old.collection_count'] = node["jvm"]["gc"]["collectors"]["old"]["collection_count"]
				metrics[first_key + '.fs.total.free_in_mbytes'] = node["fs"]["total"]["free_in_bytes"] / 1024 / 1024

		return metrics
	else:
		logging.error("Cluster data not available. Check ESCLUSTERNAME in your variables.")
		sys.exit(1)

def metricsByCluster():
	try:
		url = 'http://' + ESHOST + ':' + ESPORT + '/_cluster/health'
		payload = {'level': 'shards'}
		r = requests.get(url, params=payload, timeout=1.000)
	except requests.exceptions.RequestException as e:
		logging.exception(e)
		sys.exit(1)
	es_stats = json.loads(r.text)
	if ESCLUSTERNAME == es_stats["cluster_name"]:
		metrics = {}
		first_key = PROJECT + '.' + ENV + '.' + ESCLUSTERNAME
		if es_stats["status"] == 'green':
			status = 0
		elif es_stats["status"] == 'yellow':
			status = 1
		else:
			status = 2
		metrics[first_key + '.status'] = status
		metrics[first_key + '.active_shards'] = es_stats["active_shards"]
		metrics[first_key + '.active_primary_shards'] = es_stats["active_primary_shards"]
		metrics[first_key + '.relocating_shards'] = es_stats["relocating_shards"]
		metrics[first_key + '.initializing_shards'] = es_stats["initializing_shards"]
		metrics[first_key + '.unassigned_shards'] = es_stats["unassigned_shards"]
		return metrics
	else:
		logging.error("Cluster data not available. Check ESCLUSTERNAME in your variables.")
		sys.exit(1)

def sendToStatsd(key, value):
	stats.incr(key, value)
	#logging.debug("%s Sending to statsd - %s:%s") % (strftime("%d %b %Y %H:%M:%S", gmtime()), key, value)

if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
	parser = argparse.ArgumentParser(description='Logging statistics using Statsd, Graphite e Graphana.')
	parser.add_argument('--env', type=str, choices=['devqa', 'prod'], required=True,
                    help='Run the collect data on this environment')
	args = parser.parse_args()

	ENV = args.env
	if ENV == 'prod':
		ESHOST = os.environ['ESHOST_PROD']
		ESPORT = os.environ['ESPORT_PROD']
		ESCLUSTERNAME = os.environ['ESCLUSTERNAME_PROD']
		ESNODESNAME = os.environ['ESNODESNAME_PROD']
	else:
		ESHOST = os.environ['ESHOST_DEVQA']
		ESPORT = os.environ['ESPORT_DEVQA']
		ESCLUSTERNAME = os.environ['ESCLUSTERNAME_DEVQA']
		ESNODESNAME = os.environ['ESNODESNAME_DEVQA']

	PROJECT = os.environ['PROJECT']
	STATSD_HOST = os.environ['STATSD_HOST']
	STATSD_PORT = os.environ['STATSD_PORT']
	STATSD = statsd.StatsClient(STATSD_HOST, STATSD_PORT)
	
	s = sched.scheduler(time.time, time.sleep)
	def goahed(sc):
		for key, value in metricsByNodes().iteritems():
			sendToStatsd(key, value)
		for key, value in metricsByCluster().iteritems():
			sendToStatsd(key, value)
		sc.enter(10, 1, goahed, (sc,))
		
	s.enter(10, 1, goahed, (s,))
	s.run()
