#!/usr/bin python2.7
# -*- coding: UTF-8 -*-
# filename:  gearcenter_worker.py
# creator:   jacob.qian
# datetime:  2013-5-31
# Ronaldo gearcenter 工作线程

import time
from apnsclient import *
from pymongo import MongoClient
import OpenSSL
OpenSSL.SSL.SSLv3_METHOD = OpenSSL.SSL.TLSv1_METHOD

import sys
import subprocess
import os
import time
import datetime
import time
import threading
import json
import pymongo
import smtplib
from email.mime.text import MIMEText
import hashlib
import socket
import logging
import logging.config
import uuid
import redis
import hashlib
import urllib
import base64
import random
from bson.objectid import ObjectId
if '/opt/Keeprapid/Wishoney/server/apps/common' not in sys.path:
    sys.path.append('/opt/Keeprapid/Wishoney/server/apps/common')
import workers

logging.config.fileConfig("/opt/Keeprapid/Wishoney/server/conf/log.conf")
logger = logging.getLogger('wishoney')



class AnpsProxy(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
#        super(AnpsProxy, self).__init__()
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("AnpsProxy :running in __init__")
        self.session = Session()
#        self.apnscon = self.session.get_connection("push_sandbox", cert_file="/opt/Keeprapid/Wishoney/server/conf/apns-dev.pem")
        self.apnscon_product = self.session.get_connection("push_production", cert_file="/opt/Keeprapid/Wishoney/server/conf/apns-product-oda.pem")
#        self.srv = APNs(self.apnscon)
        self.srv_product = APNs(self.apnscon_product)

        fileobj = open('/opt/Keeprapid/Wishoney/server/conf/db.conf', 'r')
        self._json_dbcfg = json.load(fileobj)
        fileobj.close()

        fileobj = open("/opt/Keeprapid/Wishoney/server/conf/mqtt.conf", "r")
        self._mqttconfig = json.load(fileobj)
        fileobj.close()

        fileobj = open("/opt/Keeprapid/Wishoney/server/conf/config.conf", "r")
        self._config = json.load(fileobj)
        fileobj.close()
        self.thread_index = thread_index
        self.recv_queue_name = "W:Queue:ApnsOda"
        if 'apns_oda' in _config:
            if 'Consumer_Queue_Name' in _config['apns_oda']:
                self.recv_queue_name = _config['apns_oda']['Consumer_Queue_Name']
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
#        self.mongoconn = MongoClient(host=self._json_dbcfg['mongo_ip'],port=int(self._json_dbcfg['mongo_port']))
        self.mongoconn = MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.db = self.mongoconn.notify
        self.notify_log = self.db.notify_log


    def run(self):
        logger.debug("Start AnpsProxy pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
#        try:
        if 1:
            while 1:
                recvdata = self._redis.brpop(self.recv_queue_name)
                t1 = time.time()
                if recvdata:
                    self._proc_message(recvdata[1])
                logger.debug("_proc_message cost %f" % (time.time()-t1))
                    
#        except Exception as e:
#            logger.debug("%s except raised : %s " % (e.__class__, e.args))



    def _proc_message(self, recvbuf):
        '''消息处理入口函数'''
        logger.debug('_proc_message')
        #解body
        msgdict = dict()
        try:
            logger.debug(recvbuf)
            msgdict = json.loads(recvbuf)
        except:
            logger.error("parse body error")
            return
        #检查消息必选项
        if len(msgdict) == 0:
            logger.error("body lenght is zero")
            return
        if "from" not in msgdict:
            logger.error("no route in body")
            return
        msgfrom = msgdict['from']

        seqid = '0'
        if "seqid" in msgdict:
            seqid = msgdict['seqid']

        sockid = ''
        if 'sockid' in msgdict:
            sockid = msgdict['sockid']

        if "action_cmd" not in msgdict:
            logger.error("no action_cmd in msg")
            self._sendMessage(msgfrom, '{"from":%s,"error_code":"40000","seq_id":%s,"body":{},"sockid":%s)' % (self.recv_queue_name, seqid, sockid))
            return
        #构建回应消息结构
        action_cmd = msgdict['action_cmd']

        message_resp_dict = dict()
        message_resp_dict['from'] = self.recv_queue_name
        message_resp_dict['seq_id'] = seqid
        message_resp_dict['sockid'] = sockid
        message_resp_body = dict()
        message_resp_dict['body'] = message_resp_body
        
        self._proc_action(msgdict, message_resp_dict, message_resp_body)

        msg_resp = json.dumps(message_resp_dict)
        logger.debug(msg_resp)
        self._sendMessage(msgfrom, msg_resp)   

    def _proc_action(self, msg_in, msg_out_head, msg_out_body):
        '''action处理入口函数'''
#        logger.debug("_proc_action action=%s" % (action))
        if 'action_cmd' not in msg_in or 'version' not in msg_in:
            logger.error("mandotry param error in action")
            msg_out_head['error_code'] = '40002'
            return
        action_cmd = msg_in['action_cmd']
        logger.debug('action_cmd : %s' % (action_cmd))
        action_version = msg_in['version']
        logger.debug('action_version : %s' % (action_version))
        if 'body' in msg_in:
            action_body = msg_in['body']
#            logger.debug('action_body : %s' % (action_body))
        else:
            action_body = None
            logger.debug('no action_body')

        if action_cmd == 'send_anps_notify':
            self._proc_action_send_anps_notify(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = '40000'

        return




    def _proc_action_send_anps_notify(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'gear_add', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        imei    M   
                        tid O   如果有tid则只发送这一个账号
                        notify_id   M   通知内容
                        
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                }
        '''
        logger.debug(" :::[for oda] --> into _proc_action_send_anps_notify action_body:%s"%action_body)
#        if 1:
            
        if ('ios_token' not in action_body) or  ('content' not in action_body):
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return
        if action_body['ios_token'] is None or action_body['content'] is None:
            retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
            return

        ios_token = action_body['ios_token']
        memberkey = action_body['memberkey']
        badge = 0
        if 'badge' in action_body and action_body['badge'] is not None:
            badge = int(action_body['badge'])

#        token_hex = action_body['ios_token']
        payload = Message(ios_token, alert=action_body['content'], sound="alarm.caf", badge=badge)
#            if self.apns_sandbox.gateway.connection_alive is not True:
#                logger.debug("reconnect to APNS server")
#                self.apns_sandbox.force_close()
#                self.apns_sandbox = APNs(use_sandbox=True, cert_file='/opt/Keeprapid/Wishoney/server/conf/PushChatCert.pem', key_file='/opt/Keeprapid/Wishoney/server/conf/PushChatKey_nopwd.pem')

        try:
            
            res = self.srv_product.send(payload) 


        except IOError as e:
            logger.debug("IOError reconnect to APNS server")
        else:
            for token, reason in res.failed.items():
                code, errmsg = reason
                # according to APNs protocol the token reported here
                # is garbage (invalid or empty), stop using and remove it.
                logger.error("Device failed: {0}, reason: {1}".format(token, errmsg))
                logger.error("Device failed: {0}, reason: {1}".format(token, errmsg))
                memberinfo = self._redis.hgetall(memberkey)
                userprofile = eval(memberinfo['userprofile'])
                iostokenlist = userprofile['ios_token']
                iostokenlist.remove(token)
                self._redis.hset(memberkey,'userprofile',userprofile)


            # Check failures not related to devices.
            for code, errmsg in res.errors:
                logger.error("Error: {}".format(errmsg))

            # Check if there are tokens that can be retried
#            if res.needs_retry():
                # repeat with retry_message or reschedule your task
#                retry_message = res.retry()




if __name__ == "__main__":
    fileobj = open("/opt/Keeprapid/Wishoney/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()

    thread_count = 1
    if _config is not None and 'apns_oda' in _config and _config['apns_oda'] is not None:
        if 'thread_count' in _config['apns_oda'] and _config['apns_oda']['thread_count'] is not None:
            thread_count = int(_config['apns_oda']['thread_count'])

    for i in xrange(0, thread_count):
        obj = AnpsProxy(i)
        obj.setDaemon(True)
        obj.start()

    while 1:
        time.sleep(1)
