#!/bin/env python3
# -*- conding: utf-8 -*-
import json
import logging
import sys
import os
import datetime
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch


fmt = '%(asctime)s [%(name)s] %(levelname)s %(message)s'
date_fmt = '%Y-%m-%d %H:%M:%S'
logfile = '{0}.log'.format(sys.argv[0])
logging.basicConfig(format=fmt, datefmt=date_fmt, filename=logfile, level=logging.INFO)
logger = logging.getLogger()
topic = 'consumerlag'
brokers = ['99.48.234.71:9092','99.48.234.72:9092', '99.48.234.73:9092']
group = 'lag2elastic'
esIndex = 'kafkaconsumerlag'
esAddress = '99.48.234.50:9200'


if __name__ == '__main__':
    try:
        consumer = KafkaConsumer(topic, group_id = group, bootstrap_servers=brokers)
        es = Elasticsearch(esAddress)
        index_name = '{0}-{1}'.format(esIndex, datetime.datetime.utcnow().strftime("%Y.%m.%d"))
        for massage in consumer:
             msg = massage.value
             data = json.loads(msg.decode('utf-8'))
             es.index(index=index_name, doc_type='kafkalag', body=data)
             logger.debug('fetch the data %s seccuss.'%data)
    except KeyboardInterrupt:
        logger.error("Interrupted!")
        try:
            sys.exit(0)
        except SystemError:
            os._exit(0)