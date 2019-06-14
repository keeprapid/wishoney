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
import threading
import json
import pymongo

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


class DataCenter(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
#        super(DataCenter, self).__init__()
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("DataCenter :running in __init__")

        fileobj = open('/opt/Keeprapid/Wishoney/server/conf/db.conf', 'r')
        self._json_dbcfg = json.load(fileobj)
        fileobj.close()

        fileobj = open("/opt/Keeprapid/Wishoney/server/conf/config.conf", "r")
        self._config = json.load(fileobj)
        fileobj.close()
        self.thread_index = thread_index
        self.recv_queue_name = "W:Queue:DataCenter"
        if 'datacenter' in _config:
            if 'Consumer_Queue_Name' in _config['datacenter']:
                self.recv_queue_name = _config['datacenter']['Consumer_Queue_Name']

        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
#        self.locationconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.locationconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.locationdb = self.locationconn.location
        self.collect_gpsinfo = self.locationdb.gpsinfo
        self.collect_lbsinfo = self.locationdb.lbsinfo

#        self.messageconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.messageconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.messagedb = self.messageconn.message
        self.collect_message = self.messagedb.message
        self.collect_callrecord = self.messagedb.callrecord


    def run(self):
        logger.debug("Start DataCenter pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
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

        if action_cmd == 'query_gear_location':
            self._proc_action_query_gear_location(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'query_gear_message':
            self._proc_action_query_gear_message(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'query_gear_callrecord':
            self._proc_action_query_gear_callrecord(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'query_gear_alarm':
            self._proc_action_query_gear_alarm(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'query_gear_contactor':
            self._proc_action_query_gear_contactor(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = '40000'

        return

    def _proc_action_query_gear_location(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'register', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     
                        vid     
                        imei_id M   
                        start_time  O   
                        end_time    O   
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                }
        '''
        logger.debug(" into _proc_action_member_regist action_body:[%s]:%s"%(version,action_body))
        try:
            
            if ('tid' not in action_body) or  ('imei' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['tid'] is None or action_body['tid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['imei'] is None or action_body['imei'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['vid'] is None or action_body['vid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            vid = action_body['vid']
            tid = action_body['tid']
            imei_str = action_body['imei']
            imei = int(imei_str)

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            if 'device' not in tni or tni['device'] is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                return
            deviceinfo = eval(tni['device'])
            if imei_str not in deviceinfo:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                return
            resultlist = None
            if 'start_time' in action_body and action_body['start_time'] is not None and 'end_time' in action_body and action_body['end_time'] is not None:
                start_time = datetime.datetime.strptime(action_body['start_time'],"%Y-%m-%d %H:%M:%S")
                end_time = datetime.datetime.strptime(action_body['end_time'],"%Y-%m-%d %H:%M:%S")
                resultlist = self.collect_gpsinfo.find({"imei":imei, "timestamp":{"$gte":start_time, "$lte":end_time}})
            else:
                today = datetime.datetime.today()
                t = datetime.datetime(today.year, today.month, today.day,0,0,0)
                resultlist = self.collect_gpsinfo.find({"imei":imei, "timestamp":{"$gte":t}})

            if resultlist:
                location_info = list()
                retbody['location_info'] = location_info
                for data in resultlist:
                    locationdict = dict()
                    locationdict['timestamp'] = data.get('timestamp').__str__()
                    locationdict['longitude'] = data.get('longitude')
                    locationdict['vt'] = data.get('vt')
                    locationdict['altitude'] = data.get('altitude')
                    locationdict['latitude'] = data.get('latitude')
                    locationdict['location_type'] = data.get('location_type')
                    locationdict['angle'] = data.get('angle')
                    location_info.append(locationdict)

            retdict['error_code'] = self.ERRORCODE_OK
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_query_gear_message(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'login', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     
                        vid     
                        imei_id M   
                        start_time  O   
                        end_time    O   
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                   body:{
                       tid:O
                       activate_flag:O
                   }
                }
        '''
        logger.debug(" into _proc_action_query_gear_message action_body:%s"%action_body)
        try:
            
            if ('tid' not in action_body) or  ('imei' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['tid'] is None or action_body['tid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['imei'] is None or action_body['imei'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['vid'] is None or action_body['vid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            vid = action_body['vid']
            tid = action_body['tid']
            imei_str = action_body['imei']
            imei = int(imei_str)

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            if 'device' not in tni or tni['device'] is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                return
            deviceinfo = eval(tni['device'])
            if imei_str not in deviceinfo:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                return
            resultlist = None
            if 'start_time' in action_body and action_body['start_time'] is not None and 'end_time' in action_body and action_body['end_time'] is not None:
                start_time = datetime.datetime.strptime(action_body['start_time'],"%Y-%m-%d %H:%M:%S")
                end_time = datetime.datetime.strptime(action_body['end_time'],"%Y-%m-%d %H:%M:%S")
                resultlist = self.collect_message.find({"imei":imei, "timestamp":{"$gte":start_time, "$lte":end_time}})
            else:
                today = datetime.datetime.today()
                t = datetime.datetime(today.year, today.month, today.day,0,0,0)
                resultlist = self.collect_message.find({"imei":imei, "timestamp":{"$gte":t}})

            if resultlist:
                messagelist = list()
                retbody['message'] = messagelist
                for data in resultlist:
                    messagedict = dict()
                    messagedict['timestamp'] = data.get('timestamp').__str__()
                    messagedict['from'] = data.get('from')
                    messagedict['type'] = data.get('type')
                    messagedict['content'] = urllib.quote(data.get('content').encode('utf-8'))
                    messagelist.append(messagedict)

            retdict['error_code'] = self.ERRORCODE_OK
            return


        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_query_gear_callrecord(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'login', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     
                        vid     
                        imei_id M   
                        start_time  O   
                        end_time    O   
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                   body:{
                       tid:O
                       activate_flag:O
                   }
                }
        '''
        logger.debug(" into _proc_action_query_gear_callrecord action_body:%s"%action_body)
        try:
            
            if ('tid' not in action_body) or  ('imei' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['tid'] is None or action_body['tid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['imei'] is None or action_body['imei'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['vid'] is None or action_body['vid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            vid = action_body['vid']
            tid = action_body['tid']
            imei_str = action_body['imei']
            imei = int(imei_str)

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            if 'device' not in tni or tni['device'] is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                return
            deviceinfo = eval(tni['device'])
            if imei_str not in deviceinfo:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                return
            resultlist = None
            if 'start_time' in action_body and action_body['start_time'] is not None and 'end_time' in action_body and action_body['end_time'] is not None:
                start_time = datetime.datetime.strptime(action_body['start_time'],"%Y-%m-%d %H:%M:%S")
                end_time = datetime.datetime.strptime(action_body['end_time'],"%Y-%m-%d %H:%M:%S")
                resultlist = self.collect_callrecord.find({"imei":imei, "timestamp":{"$gte":start_time, "$lte":end_time}})
            else:
                today = datetime.datetime.today()
                t = datetime.datetime(today.year, today.month, today.day,0,0,0)
                resultlist = self.collect_callrecord.find({"imei":imei, "timestamp":{"$gte":t}})

            if resultlist:
                callrecordlist = list()
                retbody['callrecord'] = callrecordlist
                for data in resultlist:
                    messagedict = dict()
                    messagedict['timestamp'] = data.get('timestamp').__str__()
                    messagedict['direction'] = data.get('direction')
                    messagedict['number'] = data.get('number')
                    messagedict['answer'] = data.get('answer')
                    callrecordlist.append(messagedict)

            retdict['error_code'] = self.ERRORCODE_OK
            return


        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_query_gear_contactor(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'login', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     
                        vid     
                        imei_id M   
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                   body:{
                       tid:O
                       activate_flag:O
                   }
                }
        '''
        logger.debug(" into _proc_action_query_gear_contactor action_body:%s"%action_body)
        try:
            
            if ('tid' not in action_body) or  ('imei' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['tid'] is None or action_body['tid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['imei'] is None or action_body['imei'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['vid'] is None or action_body['vid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            vid = action_body['vid']
            tid = action_body['tid']
            imei_str = action_body['imei']
            imei = int(imei_str)

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            if 'device' not in tni or tni['device'] is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                return
            deviceinfo = eval(tni['device'])
            if imei_str not in deviceinfo:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                return
            
            searchkey = self.KEY_IMEI_ID % (imei_str, '*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                imeiinfo = self._redis.hgetall(resultlist[0])
                retbody['sos_number'] = imeiinfo.get('sos_number')
                retbody['monitor_number'] = imeiinfo.get('monitor_number')
                retbody['friend_number'] = imeiinfo.get('friend_number')
                retdict['error_code'] = self.ERRORCODE_OK
                return
            else:
                retdict['error_code'] = self.ERRORCODE_IMEI_NOT_EXIST
                return
            
            return


        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL



if __name__ == "__main__":
    fileobj = open("/opt/Keeprapid/Wishoney/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()

    thread_count = 1
    if _config is not None and 'datacenter' in _config and _config['datacenter'] is not None:
        if 'thread_count' in _config['datacenter'] and _config['datacenter']['thread_count'] is not None:
            thread_count = int(_config['datacenter']['thread_count'])

    for i in xrange(0, thread_count):
        datacenter = DataCenter(i)
        datacenter.setDaemon(True)
        datacenter.start()

    while 1:
        time.sleep(1)
