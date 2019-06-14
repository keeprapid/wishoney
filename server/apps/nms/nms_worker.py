#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:  gearcenter_worker.py
# creator:   jacob.qian
# datetime:  2013-5-31
# Ronaldo gearcenter 工作线程

import sys
import subprocess
import os
import time
import datetime
import time
if '/opt/Clousky/firearm/apps/ampqapi' not in sys.path:
    sys.path.append('/opt/Clousky/firearm/apps/ampqapi')
import rabbitmq_consume as consumer
import rabbitmq_publish as publisher

if '/opt/Clousky/Ronaldo/server/apps/utils' not in sys.path:
    sys.path.append('/opt/Clousky/Ronaldo/server/apps/utils')
import getserviceinfo

if '/opt/Clousky/Ronaldo/server/apps/common' not in sys.path:
    sys.path.append('/opt/Clousky/Ronaldo/server/apps/common')
import workers

import json
import MySQLdb as mysql
import pymongo

import logging
import logging.config
import uuid
logging.config.fileConfig("/opt/Clousky/Ronaldo/server/conf/log.conf")
logr = logging.getLogger('ronaldo')


class ipServer(workers.WorkerBase):

    def __init__(self, moduleid):
        logr.debug("ipServer :running in __init__")
        workers.WorkerBase.__init__(self, moduleid)
#       fileobj = open('/opt/Clousky/Ronaldo/server/conf/db.conf', 'r')
#       self._json_dbcfg = json.load(fileobj)
#       fileobj.close()

    def __str__(self):
        pass
        '''

        '''

    def _proc_message(self, ch, method, properties, body):
        '''
        消息处理入口函数
        '''
        logr.debug('into _proc_message')
        t1 = time.time()
        #解body
        msgdict = dict()
        try:
            logr.debug(body)
            msgdict = json.loads(body)
        except:
            logr.error("parse body error")
            self._sendMessage(properties.reply_to, self.ERROR_RSP_UNKOWN_COMMAND, properties.correlation_id)
            return
        #检查消息必选项
        if len(msgdict) == 0:
            logr.error("body lenght is zero")
            self._sendMessage(properties.reply_to, self.ERROR_RSP_UNKOWN_COMMAND, properties.correlation_id)
            return
        if "action_cmd" not in msgdict or "seq_id" not in msgdict or "version" not in msgdict:
            logr.error("no action_cmd in body")
            self._sendMessage(properties.reply_to, self.ERROR_RSP_UNKOWN_COMMAND, properties.correlation_id)
            return
        #构建回应消息结构
        message_resp = dict({'error_code':'', 'seq_id':'', 'body':{}})
        action_resp = message_resp['body']
        #根据消息中的action逐一处理
        #判断msgdict['actions']['action']是list还是dict，如果是dict说明只有一个action，如果是list说明有多个action
        resp = self._proc_action(msgdict,message_resp)

        #发送resp消息
        jsonresp = json.dumps(message_resp)
        logr.debug(jsonresp)
        logr.debug("cmd timer = %f",time.time()-t1)

        self._sendMessage(properties.reply_to, jsonresp, properties.correlation_id)

    def _proc_action(self, action, ret):
        '''action处理入口函数'''
#        ret = dict()
#        logr.debug("_proc_action action=%s" % (action))
        action_name = action['action_cmd']
        logr.debug('action_name : %s' % (action_name))
        action_version = action['version']
        logr.debug('action_version : %s' % (action_version))
        action_seqid = action['seq_id']
        logr.debug('action_seqid : %s' % (action_seqid))
        if 'body' in action:
            action_params = action['body']
#            logr.debug('action_params : %s' % (action_params))
        else:
            action_params = None
            logr.debug('no action_params')
        #构建消息返回头结构
        ret['seq_id'] = action_seqid
        if action_name == 'gear_add':
            self._proc_action_gearadd(ret, action_name, action_version, action_seqid, action_params)
        elif action_name == 'gear_auth':
            self._proc_action_gearauth(ret, action_name, action_version, action_seqid, action_params)
        elif action_name == 'gear_bind':
            self._proc_action_gearbind(ret, action_name, action_version, action_seqid, action_params)
        elif action_name == 'gear_unbind':
            self._proc_action_gearunbind(ret, action_name, action_version, action_seqid, action_params)
        else:
            ret['error_code'] = self.ERRORCODE_UNKOWN_CMD

        return

    def start_forever(self):
        logr.debug("running in start_forever")
        self._start_consumer()

    def _proc_action_gearadd(self, ret, action_name, action_version, action_seqid, action_params):
        '''
        input : {    'action_cmd'  : 'gear_add', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'vid'    : M
                        'gear_type'     : M
                        'mac_id_start'     : M
                        'mac_id_end'   : O
                        'count'    : M
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                }
        '''
        logr.debug(" into gear_add action_body:%s"%action_params)
        try:
            
            if ('vid' not in action_params) or  ('gear_type' not in action_params) or  ('mac_id_start' not in action_params) or  ('count' not in action_params):
                ret['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            
        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            ret['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

        return resp

    def check_register_info(self, user_name , password):

        if user_name == password :
            return False
        elif len(user_name) < 8:
            return False
        elif len(password) <  8:
            return False

        return True



if __name__ == "__main__":
    ''' parm1: moduleid,
    '''
    if sys.argv[1] is None:
        logr.error("Miss Parameter")
        sys.exit()
    moduleid = int(sys.argv[1])
    workers = ipServer(moduleid)
    workers.start_forever()
