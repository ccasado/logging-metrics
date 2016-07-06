#!/usr/bin/env python

import requests
import statsd
import json
import argparse
import sys
import sched
from time import gmtime, strftime
import time
import os
import logging


def metricsByNodes():
    try:
        url = 'http://' + ESHOST + ':' + ESPORT + '/_nodes/stats'
        r = requests.get(url, timeout=3.000)
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
                metrics[first_key + '.indices.query_cache.memory_size_in_bytes'] = node["indices"]["query_cache"]["memory_size_in_bytes"]
                metrics[first_key + '.indices.query_cache.evictions'] = node["indices"]["query_cache"]["evictions"]
                metrics[
                    first_key + '.jvm.mem.heap_used_percent'] = node["jvm"]["mem"]["heap_used_percent"]
                if index_total_dict[node_name] is 0:
                    continue
                else:
                    index_total = node["indices"]["indexing"]["index_total"]
                    metrics[first_key + '.indices.indexing.index_total'] = index_total - \
                        index_total_dict[node_name]
                metrics[first_key + '.indices.indexing.index_time_in_millis'] = node[
                    "indices"]["indexing"]["index_time_in_millis"]
                metrics[first_key + '.indices.search.query_time_in_millis'] = node[
                    "indices"]["search"]["query_time_in_millis"]
                if query_total_dict[node_name] is 0:
                    continue
                else:
                    query_total = node["indices"]["search"]["query_total"]
                    metrics[first_key + '.indices.search.query_total'] = query_total - \
                        query_total_dict[node_name]
                if docs_count_dict[node_name] is 0:
                    continue
                else:
                    docs_count = node["indices"]["docs"]["count"]
                    metrics[first_key + '.indices.docs.count'] = docs_count - \
                        docs_count_dict[node_name]
                metrics[
                    first_key + '.indices.segments.count'] = node["indices"]["segments"]["count"]
                metrics[first_key + '.indices.query_cache.hit_count'] = node[
                    "indices"]["query_cache"]["hit_count"]
                metrics[first_key + '.indices.query_cache.miss_count'] = node[
                    "indices"]["query_cache"]["miss_count"]
                metrics[first_key + '.jvm.gc.collectors.young.collection_count'] = node[
                    "jvm"]["gc"]["collectors"]["young"]["collection_count"]
                metrics[first_key + '.jvm.gc.collectors.old.collection_count'] = node[
                    "jvm"]["gc"]["collectors"]["old"]["collection_count"]
                metrics[first_key + '.fs.total.free_in_mbytes'] = node[
                    "fs"]["total"]["free_in_bytes"] / 1024 / 1024
        for node in es_stats["nodes"].values():
            node_name = node["name"]
            docs_count_dict[node_name] = node["indices"]["docs"]["count"]
            index_total_dict[node_name] = node["indices"]["indexing"]["index_total"]
            query_total_dict[node_name] = node["indices"]["search"]["query_total"]

        return metrics
    else:
        logging.error(
            "Cluster data not available. Check ESCLUSTERNAME in your variables.")
        sys.exit(1)


def metricsByCluster():
    try:
        url = 'http://' + ESHOST + ':' + ESPORT + '/_cluster/health'
        payload = {'level': 'shards'}
        r = requests.get(url, params=payload, timeout=3.000)
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
        metrics[first_key +
                '.active_primary_shards'] = es_stats["active_primary_shards"]
        metrics[first_key + '.relocating_shards'] = es_stats["relocating_shards"]
        metrics[first_key + '.initializing_shards'] = es_stats["initializing_shards"]
        metrics[first_key + '.unassigned_shards'] = es_stats["unassigned_shards"]
        return metrics
    else:
        logging.error(
            "Cluster data not available. Check ESCLUSTERNAME in your variables.")
        sys.exit(1)


def GraylogMetrics():
    graylog_url = "http://%s:%s/cluster/metrics/multiple" % (GRAYLOG_API_HOST, GRAYLOG_API_PORT)
    headers = {'content-type': 'application/json'}

    payload = {"metrics": [
        "org.graylog2.throughput.input.1-sec-rate",
        "org.graylog2.throughput.output.1-sec-rate",
        "org.graylog2.journal.append.1-sec-rate",
        "org.graylog2.journal.read.1-sec-rate",
        "org.graylog2.journal.segments",
        "org.graylog2.journal.entries-uncommitted",
        "jvm.memory.heap.used",
        "jvm.memory.heap.committed",
        "jvm.memory.heap.max"
    ]}
    resp = requests.post(graylog_url, data=json.dumps(payload), auth=(
        GRAYLOG_USER, GRAYLOG_PASSWORD), headers=headers)
    metrics = resp.json()
    for node in metrics:
        for m in metrics[node]['metrics']:
            bit_to_byte = 1
            key = "glog.graylog." + ENV + '.' + node + '.' + m['full_name']
            if m['full_name'].startswith('jvm.memory.heap'):
                bit_to_byte = 1024
            value = m['metric']['value']/bit_to_byte
            sendToStatsd(key, value)


def sendToStatsd(key, value):
    STATSD.incr(key, value)
    if verbose:
        print "%s Sending to statsd - %s:%s" % (strftime("%d %b %Y %H:%M:%S", gmtime()), key, value)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    parser = argparse.ArgumentParser(
        description='Logging statistics using Statsd, Graphite e Graphana.')
    parser.add_argument('--env', type=str, choices=['devqa', 'prod'], required=True,
                        help='Run the collect data on this environment')
    parser.add_argument('--verbose', action="store_true", default=False, help='Verbose')
    args = parser.parse_args()

    ENV = args.env
    verbose = args.verbose
    if ENV == 'prod':
        ESHOST = os.environ['ESHOST_PROD']
        ESPORT = os.environ['ESPORT_PROD']
        ESCLUSTERNAME = os.environ['ESCLUSTERNAME_PROD']
        ESNODESNAME = os.environ['ESNODESNAME_PROD']
        GRAYLOG_USER = os.environ['GRAYLOG_USER_PROD']
        GRAYLOG_PASSWORD = os.environ['GRAYLOG_PASSWORD_PROD']
        GRAYLOG_API_HOST = os.environ['GRAYLOG_API_HOST_PROD']
        GRAYLOG_API_PORT = os.environ['GRAYLOG_API_PORT_PROD']
    else:
        ESHOST = os.environ['ESHOST_DEVQA']
        ESPORT = os.environ['ESPORT_DEVQA']
        ESCLUSTERNAME = os.environ['ESCLUSTERNAME_DEVQA']
        ESNODESNAME = os.environ['ESNODESNAME_DEVQA']
        GRAYLOG_USER = os.environ['GRAYLOG_USER_DEVQA']
        GRAYLOG_PASSWORD = os.environ['GRAYLOG_PASSWORD_DEVQA']
        GRAYLOG_API_HOST = os.environ['GRAYLOG_API_HOST_DEVQA']
        GRAYLOG_API_PORT = os.environ['GRAYLOG_API_PORT_DEVQA']

    PROJECT = os.environ['PROJECT']
    STATSD_HOST = os.environ['STATSD_HOST']
    STATSD_PORT = os.environ['STATSD_PORT']
    STATSD = statsd.StatsClient(STATSD_HOST, STATSD_PORT)

    docs_count_dict = {}
    index_total_dict = {}
    query_total_dict = {}
    nodesname = [x.strip() for x in ESNODESNAME.split(',')]
    for n in nodesname:
        docs_count_dict[n] = 0
        index_total_dict[n] = 0
        query_total_dict[n] = 0

    s = sched.scheduler(time.time, time.sleep)

    def goahed(sc):
        GraylogMetrics()
        for key, value in metricsByNodes().iteritems():
            sendToStatsd(key, value)
        for key, value in metricsByCluster().iteritems():
            sendToStatsd(key, value)
        sc.enter(10, 1, goahed, (sc,))

    s.enter(10, 1, goahed, (s,))
    s.run()
