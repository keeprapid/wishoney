#!/usr/bin python2.7
# -*- coding: UTF-8 -*-
# filename:  gearcenter_worker.py
# creator:   jacob.qian
# datetime:  2013-5-31
# Ronaldo gearcenter 工作线程

import time
import sys
import subprocess
import os
import time
import datetime
import time
import threading
import json
import pymongo
import hashlib
import logging
import logging.config
import redis
import jpush
from bson.objectid import ObjectId
if '/opt/Keeprapid/Wishoney/server/apps/common' not in sys.path:
    sys.path.append('/opt/Keeprapid/Wishoney/server/apps/common')
import workers

logging.config.fileConfig("/opt/Keeprapid/Wishoney/server/conf/log.conf")
logger = logging.getLogger('wishoney')



class JPushProxy(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
#        super(JPushProxy, self).__init__()
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("JPushProxy :running in __init__")
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
        self.recv_queue_name = "W:Queue:JPushProxy"
        if 'jpushproxy' in _config:
            if 'Consumer_Queue_Name' in _config['jpushproxy']:
                self.recv_queue_name = _config['jpushproxy']['Consumer_Queue_Name']
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
# #        self.mongoconn = MongoClient(host=self._json_dbcfg['mongo_ip'],port=int(self._json_dbcfg['mongo_port']))
#         self.mongoconn = MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
#         self.db = self.mongoconn.notify
#         self.notify_log = self.db.notify_log
        self._jpush = jpush.JPush(self.JPUSH_APPKEY,self.JPUSH_SECRET)



    def run(self):
        logger.debug("Start JPushProxy pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
#        if 1:
        while 1:
            try:
                recvdata = self._redis.brpop(self.recv_queue_name)
                t1 = time.time()
                if recvdata:
                    self._proc_message(recvdata[1])
                logger.debug("_proc_message cost %f" % (time.time()-t1))
                    
            except Exception as e:
                logger.debug("%s except raised : %s " % (e.__class__, e.args))



    def _proc_message(self, recvbuf):
        '''消息处理入口函数'''
#        logger.debug('_proc_message')
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

        if action_cmd == 'send_jpush_notify':
            self._proc_action_send_jpush_notify(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = '40000'

        return




    def _proc_action_send_jpush_notify(self, version, action_body, retdict, retbody):
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
        logger.debug(" :::[for jpush] --> into _proc_action_send_jpush_notify action_body:%s"%action_body)
#        if 1:
        try:
            
            if ('registionId' not in action_body) or  ('content' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['registionId'] is None or action_body['content'] is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return

            registionId = action_body['registionId']
            memberkey = action_body['memberkey']
            badge = 0
            if 'badge' in action_body and action_body['badge'] is not None:
                badge = int(action_body['badge'])

    #        token_hex = action_body['ios_token']
            push = self._jpush.create_push()
            push.audience = jpush.audience(jpush.registration_id(registionId))
            push.notification = {"ios":{"alert":action_body['content'],"sound":"alarm.caf","badge":badge}}
            if 'ios_apnstype' in action_body and action_body['ios_apnstype'] == 'debug':
                push.options = {'apns_production': False}
            push.platform = jpush.all_
            logger.debug(push.payload)
        
            push.send()
        except IOError as e:
            logger.error("_proc_action_send_jpush_notify %s except raised : %s " % (e.__class__, e.args))





if __name__ == "__main__":
    fileobj = open("/opt/Keeprapid/Wishoney/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()

    thread_count = 1
    if _config is not None and 'jpushproxy' in _config and _config['jpushproxy'] is not None:
        if 'thread_count' in _config['jpushproxy'] and _config['jpushproxy']['thread_count'] is not None:
            thread_count = int(_config['jpushproxy']['thread_count'])

    for i in xrange(0, thread_count):
        obj = JPushProxy(i)
        obj.setDaemon(True)
        obj.start()

    while 1:
        time.sleep(1)
