#!/bin/env python3
# -*- conding:utf-8 -*-


import sys
import yaml
import json
import datetime
import logging
import subprocess
import time
import os
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kazoo.client import KazooClient


fmt = '%(asctime)s [%(name)s] [%(levelname)s] %(message)s'
date_fmt = '%Y-%m-%d %H:%M:%S'
logfile = '{0}.log'.format(sys.argv[0])
logging.basicConfig(format=fmt, datefmt=date_fmt, filename=logfile, level=logging.INFO)
logger = logging.getLogger(name='producer')
with open('producer.conf', 'r') as f:
    conf_file = yaml.load(f.read())
kafkaExec = conf_file["NEMO"]["kafkaExec"]
interval = conf_file["NEMO"]["interval"]
kafka_address = conf_file["NEMO"]["kafka"].split(',')
send_topic = conf_file["NEMO"]["topic"]


def get_consumer_lag(c_type, c_addr, g_name, t_name):
    """Get the topic's lag information.
       : arg c_type: the API that the consumer use,means the script to connect the brokers or zookeeper
       : arg c_addr: the address to connect,broker ip or zookeeper ip
       : arg g_name: the consumer group name to get lag information."""
    r_lag_list = []
    if c_type == 'kafka':
        lag_str = subprocess.check_output([kafkaExec, '--bootstrap-server',
                                           c_addr, '--describe', '--group', g_name]).decode('utf-8')
    elif c_type == 'zookeeper':
        lag_str = subprocess.check_output([kafkaExec, '--zookeeper',
                                           c_addr, '--descrobe', '--group', g_name]).decode('utf-8')
    else:
        return False
    lag_list = lag_str.split('\n')
    lag_list.pop()
    lag_list.pop(0)
    for con in lag_list:
        if t_name in con:
            r_lag_list.append(con)
    if len(r_lag_list) > 1:
        return r_lag_list
    else:
        return False


def get_zookeeper_offset(kafka, hosts, f_path):
    """Get the storm consume offset which saved in zookeeper"""
    r_lag_list = []
    zk = KazooClient(hosts=hosts)
    zk.start()
    consumer = KafkaConsumer(group_id='consumerlag', bootstrap_servers=kafka.split(','))
    for children in zk.get_children(f_path):
        c_path = '/'.join([f_path, children])
        b_data, stat = zk.get(c_path)
        data = json.loads(b_data.decode('utf-8'))
        tp = TopicPartition(data["topic"], data["partition"])
        consumer.assign([tp])
        consumer.committed(tp)
        consumer.seek_to_end(tp)
        last_offset = consumer.position(tp)
        lag = last_offset - data["offset"]
        data_str = 'consumerlag  {0}  {1}  {2}  {3}  {4}'.format(data["topic"], data["partition"],
                                                                      last_offset, data["offset"], lag)
        r_lag_list.append(data_str)
    return r_lag_list


def set_message_default(*data):
    """get the list from unix shell exec, format it and send to kafka"""
    utc_datetime = datetime.datetime.utcnow()
    producer = KafkaProducer(bootstrap_servers=kafka_address, value_serializer=lambda m: json.dumps(m).encode('ascii'))
    if isinstance(data, tuple):
        s_data = data[0]
    elif isinstance(data, list):
        s_data = data
    for msg in s_data:
        temp = msg.split()
        lag_info = {}
        try:
            lag_info['consumer-group'] = temp[0]
            lag_info['topic'] = temp[1]
            lag_info['partition'] = int(temp[2])
            lag_info['lag'] = int(temp[5])
            lag_info['@timestamp'] = str(utc_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])
        except ValueError:
            logger.error('The lag value was error,please check the config file for the topic name.')
            continue
        producer.send(send_topic, json.dumps(lag_info))
        logger.debug('fetch the consumer lag information success!')


def main():
    for conf in conf_file["ARONNAX"]:
        ka_addr = conf['brokerlist']
        zk_addr = conf['zookeeper']
        for consumer in conf['consumers']:
            for g_t in consumer['group-topic'].keys():
                if consumer['type'] == 'kafka':
                    lag_list = get_consumer_lag(consumer['type'], ka_addr, g_t, consumer['group-topic'][g_t])
                elif consumer['type'] == 'zookeeper':
                    lag_list = get_consumer_lag(consumer['type'], zk_addr, g_t, consumer['group-topic'][g_t])
                elif consumer['type'] == 'storm':
                    lag_list = get_zookeeper_offset(ka_addr, consumer['zookeeper'], consumer['father'])
                if lag_list:
                    set_message_default(lag_list)
                else:
                    logger.error("The group {0} of topic {1} has no lag data,"
                                 "please check the config file.".format(g_t, consumer['group-topic'][g_t]))


if __name__ == '__main__':
    try:
        nextRun = 0
        while True:
            if time.time() >= nextRun:
                nextRun = time.time() + interval
                now = time.time()
                main()
                elapsed = time.time() - now
                logger.info("Total Elapsed Time: %s" % elapsed)
                timeDiff = nextRun - time.time()
                if timeDiff >= 0:
                    time.sleep(timeDiff)
    except KeyboardInterrupt:
        logger.error('Interrupted')