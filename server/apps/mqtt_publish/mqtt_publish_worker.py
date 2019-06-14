#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:   ipserver_main.py
# creator:   jack.li
# datetime:  2014-8-18
# Ronaldo  ip 主文件
import paho.mqtt.client as mqtt

from multiprocessing import Process
import redis
import sys
import time
import uuid
import hashlib
import json
import struct
import threading
import os
import logging
import logging.config
logging.config.fileConfig("/opt/Keeprapid/Wishoney/server/conf/log.conf")
logger = logging.getLogger('wishoney')

class PublishDog(threading.Thread):
    """docstring for PublishDog"""
    def __init__(self, client, keepAlive):
        super(PublishDog, self).__init__()
        self.client = client
        self.keepAlive = keepAlive
        self.lasttime = time.time()

    def run(self):
        logger.debug("Start dog")
        try:
            while 1:
                if (time.time() - self.lasttime) > self.keepAlive:
                    logger.debug("Dog run, send ping")
                    self.client._send_pingreq()
                    self.lasttime = time.time()

                time.sleep(1)
        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))


def msg_proc(recvbuf, client):
    logger.debug(recvbuf)
    try:
#    if 1:
        msgbody = json.loads(recvbuf)
        logger.debug(msgbody)

        if 'topic' not in msgbody or msgbody['topic'] is None:
            logger.error("recvbuf format error!!!")
            return
        topic = msgbody['topic']
        if isinstance(topic, unicode):
            topic = topic.encode('ascii')

        sendbuf = None
        if 'sendbuf' in msgbody and msgbody['sendbuf'] is not None:
            sendbuf = msgbody['sendbuf']
            if 'protocol' in msgbody and 'encode' in msgbody:
                if isinstance(sendbuf, unicode):
                    if msgbody['encode'] == 'ascii':
                        sendbuf = sendbuf.encode('ascii')
                    elif msgbody['encode'] == 'utf-8':
                        sendbuf = sendbuf.encode('utf-8')

            else:
                if isinstance(sendbuf, unicode):
                    sendbuf = sendbuf.encode('ascii')
            logger.debug(sendbuf)
            client.publish(topic, sendbuf)
        else:
            if 'clock_time' in msgbody and msgbody['clock_time'] is not None and 'content' in msgbody and msgbody['content'] is not None:
                logger.debug(sys.getdefaultencoding())
                clock_time = msgbody['clock_time']
                content = msgbody['content']
#                logger.debug(content)
#                logger.debug(content.__class__)
#                logger.debug(clock_time)
#                logger.debug(clock_time.__class__)
                a = clock_time.encode('utf-8')
                b = content.encode('utf-16-le')
#                logger.debug(a)
#                logger.debug(a.__class__)
#                logger.debug(b)
#                logger.debug(b.__class__)
#                if isinstance(clock_time, unicode):
#                    clock_time = clock_time.encode('ascii')
#                if isinstance(content, unicode):
#                    content = content.encode('utf-16-le')
                sendbuf = u"generalcmd,ALARM,".encode('utf-8')+a+u','.encode('utf-8')+b+u'#'.encode('utf-8')
#                logger.debug(sendbuf)
                client.publish2(topic, sendbuf)
            elif 'setphb' in msgbody and msgbody['setphb'] is not None:
                logger.debug(sys.getdefaultencoding())
                content = msgbody['setphb']
#                logger.debug(content)
#                logger.debug(content.__class__)
#                logger.debug(clock_time)
#                logger.debug(clock_time.__class__)
#                b = content.encode('utf-16-le')
                b = content.encode('utf-8')
#                logger.debug(a)
#                logger.debug(a.__class__)
#                logger.debug(b)
#                logger.debug(b.__class__)
#                if isinstance(clock_time, unicode):
#                    clock_time = clock_time.encode('ascii')
#                if isinstance(content, unicode):
#                    content = content.encode('utf-16-le')
                sendbuf = u"generalcmd,SETPHBNAME,".encode('utf-8')+b+u';#'.encode('utf-8')
                sendbuf = sendbuf.replace("(null)","")
                logger.debug(sendbuf)
                client.publish2(topic, sendbuf)
#                sendbuf = "generalcmd,ALARM,"+a+','+b+'#'
#        logger.debug(sendbuf.__class__)

#        if sendbuf is not None:
#            client.publish(topic, sendbuf)
        

    except Exception as e:
        logger.error("%s except raised : %s " % (e.__class__, e.args))


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf8')

    fileobj = open("/opt/Keeprapid/Wishoney/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()
    fileobj = open('/opt/Keeprapid/Wishoney/server/conf/db.conf', 'r')
    _json_dbcfg = json.load(fileobj)
    fileobj.close()
    fileobj = open('/opt/Keeprapid/Wishoney/server/conf/mqtt.conf', 'r')
    _mqtt_cfg = json.load(fileobj)
    fileobj.close()

    _redis = redis.StrictRedis(_json_dbcfg['redisip'], int(_json_dbcfg['redisport']),password=_json_dbcfg['redispassword'])
    recv_queue_name = "W:Queue:MQTTPub"
    if 'mqtt_publish' in _config:
        if 'Consumer_Queue_Name' in _config['mqtt_publish']:
            recv_queue_name = _config['mqtt_publish']['Consumer_Queue_Name']

    mqttclient = mqtt.Client()
    mqttclient.connect(_mqtt_cfg['mqtt_server'], int(_mqtt_cfg['mqtt_port']), int(_mqtt_cfg['mqtt_client_timeout']))

    dog = PublishDog(mqttclient, int(_mqtt_cfg['mqtt_client_timeout'])/2) 
    dog.setDaemon(True)
    dog.start()
    logger.debug("mqtt_publish_worker kickoff... "+recv_queue_name)
    while 1:
        recvdata = _redis.brpop(recv_queue_name)
        msg_proc(recvdata[1], mqttclient)



        