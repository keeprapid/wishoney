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


class GearCenter(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
#        super(GearCenter, self).__init__()
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("GearCenter :running in __init__")

        fileobj = open("/opt/Keeprapid/Wishoney/server/conf/mqtt.conf", "r")
        self._mqttconfig = json.load(fileobj)
        fileobj.close()

        fileobj = open('/opt/Keeprapid/Wishoney/server/conf/db.conf', 'r')
        self._json_dbcfg = json.load(fileobj)
        fileobj.close()

        fileobj = open("/opt/Keeprapid/Wishoney/server/conf/config.conf", "r")
        self._config = json.load(fileobj)
        fileobj.close()
        self.thread_index = thread_index
        self.recv_queue_name = "W:Queue:GearCenter"
        if 'gearcenter' in _config:
            if 'Consumer_Queue_Name' in _config['gearcenter']:
                self.recv_queue_name = _config['gearcenter']['Consumer_Queue_Name']

#        self.mongoconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.mongoconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
        self.db = self.mongoconn.gearcenter
        self.collect_gearinfo = self.db.gearinfo


    def run(self):
        logger.debug("Start GearCenter pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
        try:
#        if 1:
            while 1:
                recvdata = self._redis.brpop(self.recv_queue_name)
                t1 = time.time()
                if recvdata:
                    self._proc_message(recvdata[1])
                logger.debug("_proc_message cost %f" % (time.time()-t1))
        except Exception as e:
            logger.debug("%s except raised : %s " % (e.__class__, e.args))

#    def send_to_publish_queue(self, sendbuf):
#        '''sendbuf : dict'''
#        self._redis.lpush(self.publish_queue_name, json.dumps(sendbuf))

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
#        logger.debug(msg_resp)
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

        if action_cmd == 'gear_add':
            self._proc_action_gear_add(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'gear_add2':
            self._proc_action_gear_add2(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'gear_info':
            self._proc_action_gear_info(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'gear_set_number':
            self._proc_action_gear_set_number(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'gear_called':
            self._proc_action_gear_called(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'gear_set_clock':
            self._proc_action_gear_set_clock(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'gear_start_location':
            self._proc_action_gear_start_location(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'gear_set_alertmode':
            self._proc_action_gear_set_alertmode(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'gear_call_back':
            self._proc_action_gear_call_back(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'gear_off':
            self._proc_action_gear_off(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'gear_set_param':
            self._proc_action_gear_set_param(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = '40000'

        return

    def _proc_action_gear_add(self, version, action_body, retdict, retbody):
        ''' input:
            vid  M   厂家编码 参见附录
            gear_type   M   设备类型编码 参见附录
            imei_start  M   
            imei_end    M  

            output:
            gear_count  O   添加的条数
            badimei     

            '''
        logger.debug("_proc_action_gear_add")
        #检查参数
        if action_body is None or ('gear_type' not in action_body) or  ('imei_start' not in action_body) or  ('imei_end' not in action_body) or ('vid' not in action_body):
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return
        if 'gear_type' not in action_body or action_body['gear_type'] is None:
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return
        if 'imei_start' not in action_body or action_body['imei_start'] is None:
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return
        if 'imei_end' not in action_body or action_body['imei_end'] is None:
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return
        if 'vid' not in action_body or action_body['vid'] is None:
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return


        try:
            gear_type = action_body['gear_type']
            vid = action_body['vid']
            imei_start = int(action_body['imei_start'])
            imei_end = int(action_body['imei_end'])

            if imei_start>imei_end:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            count = 0
            
            existimeilist = list()
            resultlist = self.collect_gearinfo.find({'imei':{'$gte':imei_start,'$lte':imei_end}},fields=['imei'])
            for gearinfo in resultlist:
                existimeilist.append(gearinfo['imei'])

#            logger.debug(existimeilist)
            for imei in xrange(imei_start, imei_end+1):
#                logger.debug(imei)
                if imei in existimeilist:
                    continue
                insertimei = dict({\
                    'imei':imei,\
                    'vid':vid,\
                    'createtime': datetime.datetime.now(),\
                    'activetime':None,\
                    'state': self.AUTH_STATE_NEW,\
                    'maxfollowcount':self.GEAR_MAX_FOLLOW_COUNT,\
                    'nickname':'',\
                    'height':0,\
                    'weight':0,\
                    'birthday':'',\
                    'gender':'',\
                    'stride':0,\
                    'mobile':'',\
                    'battery_level':0,\
                    'gsm_level':0,\
                    'last_location_type':0,\
                    'last_long':0,\
                    'last_lat':0,\
                    'last_vt':0,\
                    'last_angel':0,\
                    'last_location_objectid':None,\
                    'alarm_enable':1,\
                    'fence_list':dict(),\
                    'alarm_lat':0,\
                    'alarm_long':0,\
                    'alarm_radius':0,\
                    'sos_number':'',\
                    'monitor_number':'',\
                    'friend_number':'',\
                    'img_url':'',\
                    'alert_motor':'1',\
                    'alert_ring':'1',\
                    'follow':list()\
                    })
                ret = self.collect_gearinfo.insert_one(insertimei)
                if ret is not None:
                    insertimei['createtime'] = insertimei['createtime'].__str__()
                    key = self.KEY_IMEI_ID % (str(imei), ret.inserted_id.__str__())
                    self._redis.hmset(key, insertimei)
                count += 1


            retdict['error_code'] = '200'
            retbody['badmacid'] = ','.join(str(x) for x in existimeilist)
            retbody['gear_count'] = count

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = '40001'
            return


    def _proc_action_gear_add2(self, version, action_body, retdict, retbody):
        ''' input:
            vid  M   厂家编码 参见附录
            gear_type   M   设备类型编码 参见附录
            imei_start  M   
            imei_end    M  

            output:
            gear_count  O   添加的条数
            badimei     

            '''
        logger.debug("_proc_action_gear_add")
        #检查参数
        if action_body is None or ('gear_type' not in action_body) or  ('imei_list' not in action_body) or ('vid' not in action_body):
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return
        if 'gear_type' not in action_body or action_body['gear_type'] is None:
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return
        if 'imei_list' not in action_body or action_body['imei_list'] is None:
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return
        if 'vid' not in action_body or action_body['vid'] is None:
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return


        try:
            gear_type = action_body['gear_type']
            vid = action_body['vid']
            tmpimei_list = action_body['imei_list'].split(',')
            count = 0

            imei_start = 0
            imei_end = 0
            imeilist = list()
            for imeistr in tmpimei_list:
                imei = int(imeistr)
                if imei>imei_start:
                    imei_start = imei
                if imei_end == 0:
                    imei_end = imei
                if imei < imei_end:
                    imei_end = imei
                imeilist.append(imei)

            existimeilist = list()
            resultlist = self.collect_gearinfo.find({'imei':{'$gte':imei_start,'$lte':imei_end}},fields=['imei'])
            for gearinfo in resultlist:
                existimeilist.append(gearinfo['imei'])

#            logger.debug(existimeilist)
            for imei in imeilist:
#                logger.debug(imei)
                if imei in existimeilist:
                    continue
                insertimei = dict({\
                    'imei':imei,\
                    'vid':vid,\
                    'createtime': datetime.datetime.now(),\
                    'activetime':None,\
                    'state': self.AUTH_STATE_NEW,\
                    'maxfollowcount':self.GEAR_MAX_FOLLOW_COUNT,\
                    'nickname':'',\
                    'height':0,\
                    'weight':0,\
                    'birthday':'',\
                    'gender':'',\
                    'stride':0,\
                    'mobile':'',\
                    'battery_level':0,\
                    'gsm_level':0,\
                    'last_location_type':0,\
                    'last_long':0,\
                    'last_lat':0,\
                    'last_vt':0,\
                    'last_angel':0,\
                    'last_location_objectid':None,\
                    'alarm_enable':1,\
                    'fence_list':dict(),\
                    'alarm_lat':0,\
                    'alarm_long':0,\
                    'alarm_radius':0,\
                    'sos_number':'',\
                    'monitor_number':'',\
                    'friend_number':'',\
                    'img_url':'',\
                    'alert_motor':'1',\
                    'alert_ring':'1',\
                    'follow':list()\
                    })
                ret = self.collect_gearinfo.insert_one(insertimei)
                if ret is not None:
                    insertimei['createtime'] = insertimei['createtime'].__str__()
                    key = self.KEY_IMEI_ID % (str(imei), ret.inserted_id.__str__())
                    self._redis.hmset(key, insertimei)
                count += 1


            retdict['error_code'] = '200'
            retbody['badmacid'] = ','.join(str(x) for x in existimeilist)
            retbody['gear_count'] = count

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = '40001'
            return


    def _proc_action_gear_info(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'register', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     
                        vid     
                        imei 
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                }
        '''
        logger.debug(" into _proc_action_gear_info action_body:[%s]:%s"%(version,action_body))
        try:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or  ('imei' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['tid'] is None or action_body['tid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['vid'] is None or action_body['vid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['imei'] is None or action_body['imei'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            vid = action_body['vid'];
            tid = action_body['tid'];
            imei_str = action_body['imei'];
            imei = int(action_body['imei'])
            if vid != self.ADMIN_VID:
                searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
                resultlist = self._redis.keys(searchkey)
                if len(resultlist) == 0:
                    logger.error("No found user token %s" %(tid))
                    retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                    return

                userinfo = self._redis.hgetall(resultlist[0])
                if 'device' not in userinfo or userinfo['device'] is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                    return
                imeidict = eval(userinfo['device'])
                if imei_str not in imeidict:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                    return
                memberimeiinfo = imeidict[imei_str]
                if 'relationship' in memberimeiinfo and memberimeiinfo['relationship'] is not None:
                    retbody['relationship'] = urllib.quote(memberimeiinfo['relationship'].encode('utf-8'))
                else:
                    retbody['relationship'] = ''

            searchkey = self.KEY_IMEI_ID % (imei_str, '*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                imeiinfo = self._redis.hgetall(resultlist[0])
                for key in imeiinfo:
                    retbody[key] = imeiinfo[key]
            else:
                imeiinfo = self.collect_gearinfo.find_one({"imei":imei})
                if imeiinfo == None:
                    retdict['error_code'] = self.ERRORCODE_IMEI_NOT_EXIST
                    return

                addkey = self.KEY_IMEI_ID % (imei_str, imeiinfo['_id'].__str__())
                for key in imeiinfo:
                    if key == '_id':
                        continue
                    elif key in ['nickname','img_url']:
                        self._redis.hset(addkey, key, imeiinfo[key])
                        retbody[key] = urllib.quote(imeiinfo[key].encode('utf-8'))
                    elif key == 'follow':
                        if imeiinfo[key] == None:
                            self._redis.hset(addkey, key, '[]')
                            retbody[key] = ''
                        else:
                            self._redis.hset(addkey, key, imeiinfo[key])
                            retbody[key] = tempstr
                    elif key in ['createtime','activetime']:
                        self._redis.hset(addkey, key, imeiinfo[key].__str__())
                        retbody[key] = imeiinfo[key].__str__()
                    else:
                        self._redis.hset(addkey, key, imeiinfo[key])
                        retbody[key] = imeiinfo[key]

            retdict['error_code'] = self.ERRORCODE_OK
            return


        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_gear_set_number(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'login', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     
                        vid     
                        imei_id M   
                        sos_number  M   number1,number2,number3
                        monitor_number  M   number1,number2
                        friend_number   M   number1,number2
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
        logger.debug(" into _proc_action_gear_set_number action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or  ('imei' not in action_body) or ('sos_number' not in action_body) or  ('monitor_number' not in action_body) or  ('friend_number' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['tid'] is None or action_body['tid'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['vid'] is None or action_body['vid'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['imei'] is None or action_body['imei'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['sos_number'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['monitor_number'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['friend_number'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            vid = action_body['vid'];
            tid = action_body['tid'];
            imei_str = action_body['imei'];
            imei = int(action_body['imei'])
            sos_number = action_body['sos_number'].replace("(null)","");
            monitor_number = action_body['monitor_number'].replace("(null)","");
            friend_number = action_body['friend_number'].replace("(null)","");

            if vid != self.ADMIN_VID:
                searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
                resultlist = self._redis.keys(searchkey)
                if len(resultlist) == 0:
                    logger.error("No found user token %s" %(tid))
                    retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                    return

                userinfo = self._redis.hgetall(resultlist[0])
                if 'device' not in userinfo or userinfo['device'] is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                    return
                imeidict = eval(userinfo['device'])
                if imei_str not in imeidict:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                    return

            imeiinfo = None
            searchkey = self.KEY_IMEI_ID % (imei_str,'*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist) == 0:            
                imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
                if imeiinfo == None:
                    logger.error("can't found imei in cache!!!")
                    retdict['error_code'] = self.ERRORCODE_IMEI_NOT_EXIST
                    return
                else:
                    imei_id = imeiinfo['_id'].__str__()
                    imeikey = self.KEY_IMEI_ID % (imei_str,imei_id)
                    for key in imeiinfo:
                        if key in ['_id','createtime']:
                            imeiinfo[key] = imeiinfo[key].__str__()

                    self._redis.hmset(imeikey, imeiinfo)
                    logger.error("get back from db imei:%s"%(imeikey))
            else:
                imeikey = resultlist[0]
                imeiinfo = self._redis.hgetall(imeikey)


            cmdkey = self.KEY_IMEI_CMD % (imei_str, self.CMD_TYPE_SET_NUMBER)
            if self._redis.exists(cmdkey) is True:
                retdict['error_code'] = self.ERRORCODE_IMEI_HAS_SAME_CMD_NOT_RESPONE
                return

            if 'protocol' in imeiinfo and imeiinfo['protocol'] == self.PROTOCOL_VERSION_2:
                soslisttmp = sos_number.split(',')[0:3]
                monitortmplist = monitor_number.split(',')[0:2]
                friendtmplist = friend_number.split(',')[0:5]
                sosnamelist = sos_number.split(',')[3:]
                monitornamelist = monitor_number.split(',')[2:]
                friendnamelist = friend_number.split(',')[5:]
                for x in range(len(sosnamelist),3):
                    sosnamelist.append('')
                for x in range(len(monitornamelist),2):
                    monitornamelist.append('')
                for x in range(len(friendnamelist),5):
                    friendnamelist.append('')

                sendbuf = "SETNBR:3,%s|%s,%s|%s,%s|%s,2,%s|%s,%s|%s,5,%s|%s,%s|%s,%s|%s,%s|%s,%s|%s#" % (\
                    soslisttmp[0],urllib.unquote(sosnamelist[0].encode('utf-8')).decode('utf-8'),\
                    soslisttmp[1],urllib.unquote(sosnamelist[1].encode('utf-8')).decode('utf-8'),\
                    soslisttmp[2],urllib.unquote(sosnamelist[2].encode('utf-8')).decode('utf-8'),\
                    monitortmplist[0],urllib.unquote(monitornamelist[0].encode('utf-8')).decode('utf-8'),\
                    monitortmplist[1],urllib.unquote(monitornamelist[1].encode('utf-8')).decode('utf-8'),\
                    friendtmplist[0],urllib.unquote(friendnamelist[0].encode('utf-8')).decode('utf-8'),\
                    friendtmplist[1],urllib.unquote(friendnamelist[1].encode('utf-8')).decode('utf-8'),\
                    friendtmplist[2],urllib.unquote(friendnamelist[2].encode('utf-8')).decode('utf-8'),\
                    friendtmplist[3],urllib.unquote(friendnamelist[3].encode('utf-8')).decode('utf-8'),\
                    friendtmplist[4],urllib.unquote(friendnamelist[4].encode('utf-8')).decode('utf-8'))
                senddict = dict()
                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
                senddict['sendbuf'] = sendbuf.encode('utf-8')
                senddict['encode'] = 'utf-8'
                senddict['protocol'] = self.PROTOCOL_VERSION_2



            else:
                soslisttmp = sos_number.split(',')
                namelist =list()
                soslist = list()
                for x in range(0,3):
                    soslist.append(soslisttmp[x])
                    if len(soslisttmp) >= x+1+3:
                        namelist.append("%s,%s" %(soslisttmp[x], urllib.unquote(soslisttmp[x+3].encode('utf-8')).decode('utf-8')))
                    else:
                        namelist.append("%s," %(soslisttmp[x]))

                monitortmplist = monitor_number.split(',')
                monitorlist = list()
                for x in range(0,2):
                    monitorlist.append(monitortmplist[x])
                    if len(monitortmplist) >= x+1+2:
                        namelist.append("%s,%s" %(monitorlist[x], urllib.unquote(monitortmplist[x+2].encode('utf-8')).decode('utf-8')))
                    else:
                        namelist.append("%s," %(monitorlist[x]))


                friendtmplist = friend_number.split(',')
                friendlist = list()
                for x in range(0,5):
                    friendlist.append(friendtmplist[x])
                    if len(friendtmplist) >= x+1+5:
                        namelist.append("%s,%s" %(friendtmplist[x], urllib.unquote(friendtmplist[x+5].encode('utf-8')).decode('utf-8')))
                    else:
                        namelist.append("%s," %(friendtmplist[x]))

                senddict = dict()
                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
                senddict['setphb'] = "%s" % (';'.join(namelist))
                if 'mqtt_publish' in self._config:
                    self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))


                senddict = dict()
                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
                senddict['sendbuf'] = "generalcmd,SOS,%s,MONITOR,%s,FRIEND,%s,#" % (','.join(soslist), ','.join(monitorlist), ','.join(friendlist))

            #先缓存消息到redis，等待回应或者等待下次发送
            self._redis.hmset(cmdkey, senddict)
            self._redis.hset(cmdkey, 'soslist', sos_number)
            self._redis.hset(cmdkey, 'monitorlist', monitor_number)
            self._redis.hset(cmdkey, 'friendlist', friend_number)
            self._redis.hset(cmdkey,'tid',tid)
            self._redis.expire(cmdkey,self.UNSEND_MSG_TIMEOUT)

            onlinekey = self.KEY_IMEI_ONLINE_FLAG % (imei_str)
            if self._redis.exists(onlinekey) is False:
                #机器不在线退出
                retdict['error_code'] = self.ERRORCODE_IMEI_STATE_OOS
                return

            #发送消息
            if 'mqtt_publish' in self._config:
                self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))


            retdict['error_code'] = self.ERRORCODE_OK
            return


        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_gear_called(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_gear_called action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            tid = action_body['tid']
            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist):
                self.redisdelete(resultlist)

            retdict['error_code'] = '200'
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_gear_set_clock(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                        'uid'   : O
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_gear_set_clock action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or  ('imei' not in action_body) or ('clock_time' not in action_body) or  ('content' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['tid'] is None or action_body['tid'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['vid'] is None or action_body['vid'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['imei'] is None or action_body['imei'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['clock_time'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['content'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            vid = action_body['vid']
            tid = action_body['tid']
            imei_str = action_body['imei']
            imei = int(action_body['imei'])
            clock_time = action_body['clock_time']
#            logger.debug(action_body['content'].__class__)
            content = urllib.unquote(action_body['content'].encode('utf-8')).decode('utf-8')
#            logger.debug(content.__class__)
            userinfo = dict()
            if vid != self.ADMIN_VID:
                searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
                resultlist = self._redis.keys(searchkey)
                if len(resultlist) == 0:
                    logger.error("No found user token %s" %(tid))
                    retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                    return

                userinfo = self._redis.hgetall(resultlist[0])
                if 'device' not in userinfo or userinfo['device'] is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                    return
                imeidict = eval(userinfo['device'])
                if imei_str not in imeidict:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                    return

            imeiinfo = None
            searchkey = self.KEY_IMEI_ID % (imei_str,'*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist) == 0:            
                imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
                if imeiinfo == None:
                    logger.error("can't found imei in cache!!!")
                    retdict['error_code'] = self.ERRORCODE_IMEI_NOT_EXIST
                    return
                else:
                    imei_id = imeiinfo['_id'].__str__()
                    imeikey = self.KEY_IMEI_ID % (imei_str,imei_id)
                    for key in imeiinfo:
                        if key in ['_id','createtime']:
                            imeiinfo[key] = imeiinfo[key].__str__()

                    self._redis.hmset(imeikey, imeiinfo)
                    logger.error("get back from db imei:%s"%(imeikey))
            else:
                imeikey = resultlist[0]
                imeiinfo = self._redis.hgetall(imeikey)

            cmdkey = self.KEY_IMEI_CMD % (imei_str, self.CMD_TYPE_SET_CLOCK)
            if self._redis.exists(cmdkey) is True:
                retdict['error_code'] = self.ERRORCODE_IMEI_HAS_SAME_CMD_NOT_RESPONE
                return

            senddict = dict()
            if 'protocol' in imeiinfo and imeiinfo['protocol'] == self.PROTOCOL_VERSION_2:
                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
                timelist = clock_time.encode("utf-8").split('-')
                timestr = "%s-%s-%s %s:00" % (timelist[0],timelist[1],timelist[2],timelist[3])
                content = urllib.unquote(action_body['content'].encode('utf-8')).decode('utf-8')
                logger.debug(content)
                senddict['sendbuf'] = (u"AL:%s,%s#"%(timestr,content)).encode('utf-8')
                senddict['encode'] = 'utf-8'
                senddict['protocol'] = self.PROTOCOL_VERSION_2
                logger.debug(senddict)
            else:
                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
                contentbe = content.encode('utf-16-be')
    #            logger.debug(contentbe)
    #            logger.debug(clock_time.__class__)
                senddict['clock_time'] = clock_time
                senddict['content'] = content
    #            senddict['sendbuf'] = "generalcmd,ALARM,"+(clock_time)+','+ contentbe + '#'
                logger.debug(senddict)

            #先缓存消息到redis，等待回应或者等待下次发送
            self._redis.hmset(cmdkey, senddict)
            self._redis.hset(cmdkey,'tid',tid)
            self._redis.hset(cmdkey,'firedate', clock_time)
            self._redis.hset(cmdkey,'content',content)
            self._redis.hset(cmdkey,'memberid',userinfo.get('_id'))
            self._redis.expire(cmdkey,self.UNSEND_MSG_TIMEOUT)

            onlinekey = self.KEY_IMEI_ONLINE_FLAG % (imei_str)
            if self._redis.exists(onlinekey) is False:
                #机器不在线退出
                retdict['error_code'] = self.ERRORCODE_IMEI_STATE_OOS
                return

            #发送消息
            if 'mqtt_publish' in self._config:
                self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))


            retdict['error_code'] = self.ERRORCODE_OK
            return
        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_gear_start_location(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_update', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                        'email'   : O
                        'mobile'  : O
                        'nickname': O
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_gear_start_location action_body:%s"%action_body)
        try:
            if ('tid' not in action_body) or  ('vid' not in action_body) or  ('imei' not in action_body) :
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['tid'] is None or action_body['tid'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['vid'] is None or action_body['vid'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['imei'] is None or action_body['imei'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            vid = action_body['vid'];
            tid = action_body['tid'];
            imei_str = action_body['imei'];
            imei = int(action_body['imei'])
            userinfo = dict()
            if vid != self.ADMIN_VID:
                searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
                resultlist = self._redis.keys(searchkey)
                if len(resultlist) == 0:
                    logger.error("No found user token %s" %(tid))
                    retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                    return

                userinfo = self._redis.hgetall(resultlist[0])
                if 'device' not in userinfo or userinfo['device'] is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                    return
                imeidict = eval(userinfo['device'])
                if imei_str not in imeidict:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                    return

            imeiinfo = None
            searchkey = self.KEY_IMEI_ID % (imei_str,'*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist) == 0:            
                imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
                if imeiinfo == None:
                    logger.error("can't found imei in cache!!!")
                    retdict['error_code'] = self.ERRORCODE_IMEI_NOT_EXIST
                    return
                else:
                    imei_id = imeiinfo['_id'].__str__()
                    imeikey = self.KEY_IMEI_ID % (imei_str,imei_id)
                    for key in imeiinfo:
                        if key in ['_id','createtime']:
                            imeiinfo[key] = imeiinfo[key].__str__()

                    self._redis.hmset(imeikey, imeiinfo)
                    logger.error("get back from db imei:%s"%(imeikey))
            else:
                imeikey = resultlist[0]
                imeiinfo = self._redis.hgetall(imeikey)

            cmdkey = self.KEY_IMEI_CMD % (imei_str, self.CMD_TYPE_START_LOCATION)
            senddict = dict()

            if 'protocol' in imeiinfo and imeiinfo['protocol'] == self.PROTOCOL_VERSION_2:
                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
                senddict['sendbuf'] = "StartLocate:0#"
                senddict['encode'] = 'ascii'
                senddict['protocol'] = self.PROTOCOL_VERSION_2
            else:
                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
                senddict['sendbuf'] = "generalcmd,SetModeToEmergency#"

            #先缓存消息到redis，等待回应或者等待下次发送
            self._redis.hmset(cmdkey, senddict)
            self._redis.hset(cmdkey,'tid',tid)
            self._redis.expire(cmdkey,self.UNSEND_MSG_TIMEOUT)

            onlinekey = self.KEY_IMEI_ONLINE_FLAG % (imei_str)
            if self._redis.exists(onlinekey) is False:
                #机器不在线退出
                retdict['error_code'] = self.ERRORCODE_IMEI_STATE_OOS
                return

            #发送消息
            if 'mqtt_publish' in self._config:
                self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))


            retdict['error_code'] = self.ERRORCODE_OK
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL



    def _proc_action_gear_set_alertmode(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_update', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                        'email'   : O
                        'mobile'  : O
                        'nickname': O
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_gear_set_alertmode action_body:%s"%action_body)
        try:
            if ('tid' not in action_body) or  ('vid' not in action_body) or  ('imei' not in action_body) or  ('alert_motor' not in action_body) or  ('alert_ring' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['tid'] is None or action_body['tid'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['vid'] is None or action_body['vid'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['imei'] is None or action_body['imei'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['alert_motor'] is None or action_body['alert_motor'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['alert_ring'] is None or action_body['alert_ring'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            alert_motor = action_body['alert_motor']
            alert_ring = action_body['alert_ring']

            vid = action_body['vid'];
            tid = action_body['tid'];
            imei_str = action_body['imei'];
            imei = int(action_body['imei'])
            userinfo = dict()

            imeiinfo = None
            searchkey = self.KEY_IMEI_ID % (imei_str,'*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist) == 0:            
                imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
                if imeiinfo == None:
                    logger.error("can't found imei in cache!!!")
                    retdict['error_code'] = self.ERRORCODE_IMEI_NOT_EXIST
                    return
                else:
                    imei_id = imeiinfo['_id'].__str__()
                    imeikey = self.KEY_IMEI_ID % (imei_str,imei_id)
                    for key in imeiinfo:
                        if key in ['_id','createtime']:
                            imeiinfo[key] = imeiinfo[key].__str__()

                    self._redis.hmset(imeikey, imeiinfo)
                    logger.error("get back from db imei:%s"%(imeikey))
            else:
                imeikey = resultlist[0]
                imeiinfo = self._redis.hgetall(imeikey)


            if vid != self.ADMIN_VID:
                searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
                resultlist = self._redis.keys(searchkey)
                if len(resultlist) == 0:
                    logger.error("No found user token %s" %(tid))
                    retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                    return

                userinfo = self._redis.hgetall(resultlist[0])
                if 'device' not in userinfo or userinfo['device'] is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                    return
                imeidict = eval(userinfo['device'])
                if imei_str not in imeidict:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                    return


            onlinekey = self.KEY_IMEI_ONLINE_FLAG % (imei_str)
            if self._redis.exists(onlinekey) is False:
                #机器不在线退出
                retdict['error_code'] = self.ERRORCODE_IMEI_STATE_OOS
                return

            searchkey = self.KEY_IMEI_ID % (imei_str,'*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist) == 0:
                retdict['error_code'] = self.ERRORCODE_IMEI_STATE_OOS
                return

            imeikey = resultlist[0]
            self._redis.hset(imeikey,'alert_motor', alert_motor)
            self._redis.hset(imeikey,'alert_ring', alert_ring)
            self.collect_gearinfo.update_one({'imei':imei},{'$set':{'alert_ring':alert_ring, 'alert_motor':alert_motor}})
            
            senddict = dict()

            if 'protocol' in imeiinfo and imeiinfo['protocol'] == self.PROTOCOL_VERSION_2:
                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
                mute = "0"
                if alert_ring == "1":
                    mute = "0"
                else:
                    mute = "1"
                senddict['sendbuf'] = "SETPARAM:vib|%s,mute|%s#"%(alert_motor, mute)
                senddict['encode'] = 'ascii'
                senddict['protocol'] = self.PROTOCOL_VERSION_2

            else:
                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
                senddict['sendbuf'] = "generalcmd,SETALERTMODE,alert_motor_onoff=%s,alert_ring_onoff=%s#" % (alert_motor, alert_ring)

            #发送消息
            if 'mqtt_publish' in self._config:
                self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))



            retdict['error_code'] = self.ERRORCODE_OK
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_gear_call_back(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_update', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                        'email'   : O
                        'mobile'  : O
                        'nickname': O
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_gear_call_back action_body:%s"%action_body)
        try:
            if ('tid' not in action_body) or  ('vid' not in action_body) or  ('imei' not in action_body) :
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['tid'] is None or action_body['tid'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['vid'] is None or action_body['vid'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['imei'] is None or action_body['imei'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            vid = action_body['vid'];
            tid = action_body['tid'];
            imei_str = action_body['imei'];
            imei = int(action_body['imei'])
            number = None
            if 'number' in action_body and action_body['number'] is not None:
                number = action_body['number']

            userinfo = dict()
            if vid != self.ADMIN_VID:
                searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
                resultlist = self._redis.keys(searchkey)
                if len(resultlist) == 0:
                    logger.error("No found user token %s" %(tid))
                    retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                    return

                userinfo = self._redis.hgetall(resultlist[0])
                if 'device' not in userinfo or userinfo['device'] is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                    return
                imeidict = eval(userinfo['device'])
                if imei_str not in imeidict:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                    return

            imeiinfo = None
            searchkey = self.KEY_IMEI_ID % (imei_str,'*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist) == 0:            
                imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
                if imeiinfo == None:
                    logger.error("can't found imei in cache!!!")
                    retdict['error_code'] = self.ERRORCODE_IMEI_NOT_EXIST
                    return
                else:
                    imei_id = imeiinfo['_id'].__str__()
                    imeikey = self.KEY_IMEI_ID % (imei_str,imei_id)
                    for key in imeiinfo:
                        if key in ['_id','createtime']:
                            imeiinfo[key] = imeiinfo[key].__str__()

                    self._redis.hmset(imeikey, imeiinfo)
                    logger.error("get back from db imei:%s"%(imeikey))
            else:
                imeikey = resultlist[0]
                imeiinfo = self._redis.hgetall(imeikey)
            if number is not None:
                if 'monitor_number' not in imeiinfo or imeiinfo['monitor_number'] == '':
                    retdict['error_code'] = self.ERRORCODE_IMEI_HAS_NO_NUMBER
                    return
                monitorlist = imeiinfo['monitor_number'].split(',')
                if number not in monitorlist:
                    retdict['error_code'] = self.ERRORCODE_IMEI_HAS_NO_NUMBER
                    return

            senddict = dict()

            if 'protocol' in imeiinfo and imeiinfo['protocol'] == self.PROTOCOL_VERSION_2:
                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
                senddict['encode'] = 'ascii'
                senddict['protocol'] = self.PROTOCOL_VERSION_2                
                if number is not None:
                    senddict['sendbuf'] = "CB:%s#" % (number)

                else:
                    senddict['sendbuf'] = "CB:#"

            onlinekey = self.KEY_IMEI_ONLINE_FLAG % (imei_str)
            if self._redis.exists(onlinekey) is False:
                #机器不在线退出
                retdict['error_code'] = self.ERRORCODE_IMEI_STATE_OOS
                return

            #发送消息
            if 'mqtt_publish' in self._config:
                self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))


            retdict['error_code'] = self.ERRORCODE_OK
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_gear_off(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_update', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                        'email'   : O
                        'mobile'  : O
                        'nickname': O
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_gear_off action_body:%s"%action_body)
        try:
            if ('tid' not in action_body) or  ('vid' not in action_body) or  ('imei' not in action_body) :
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['tid'] is None or action_body['tid'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['vid'] is None or action_body['vid'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['imei'] is None or action_body['imei'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            vid = action_body['vid'];
            tid = action_body['tid'];
            imei_str = action_body['imei'];
            imei = int(action_body['imei'])

            userinfo = dict()
            if vid != self.ADMIN_VID:
                searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
                resultlist = self._redis.keys(searchkey)
                if len(resultlist) == 0:
                    logger.error("No found user token %s" %(tid))
                    retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                    return

                userinfo = self._redis.hgetall(resultlist[0])
                member_id = userinfo['_id']
                if 'device' not in userinfo or userinfo['device'] is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                    return
                imeidict = eval(userinfo['device'])
                if imei_str not in imeidict:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                    return


                imeiinfo = None
                searchkey = self.KEY_IMEI_ID % (imei_str,'*')
                resultlist = self._redis.keys(searchkey)
                if len(resultlist) == 0:            
                    imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
                    if imeiinfo == None:
                        logger.error("can't found imei in cache!!!")
                        retdict['error_code'] = self.ERRORCODE_IMEI_NOT_EXIST
                        return
                    else:
                        imei_id = imeiinfo['_id'].__str__()
                        imeikey = self.KEY_IMEI_ID % (imei_str,imei_id)
                        for key in imeiinfo:
                            if key in ['_id','createtime']:
                                imeiinfo[key] = imeiinfo[key].__str__()

                        self._redis.hmset(imeikey, imeiinfo)
                        logger.error("get back from db imei:%s"%(imeikey))
                else:
                    imeikey = resultlist[0]
                    imeiinfo = self._redis.hgetall(imeikey)

                if imeiinfo['owner'] != member_id:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_OWNER
                    return
               

            senddict = dict()

            if 'protocol' in imeiinfo and imeiinfo['protocol'] == self.PROTOCOL_VERSION_2:
                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
                senddict['sendbuf'] = "POWEROFF:#"
                senddict['encode'] = 'ascii'
                senddict['protocol'] = self.PROTOCOL_VERSION_2


            onlinekey = self.KEY_IMEI_ONLINE_FLAG % (imei_str)
            if self._redis.exists(onlinekey) is False:
                #机器不在线退出
                retdict['error_code'] = self.ERRORCODE_IMEI_STATE_OOS
                return

            #发送消息
            if 'mqtt_publish' in self._config:
                self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))


            retdict['error_code'] = self.ERRORCODE_OK
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_gear_set_param(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : '_proc_action_gear_set_param', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                        'email'   : O
                        'mobile'  : O
                        'nickname': O
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_gear_set_param action_body:%s"%action_body)
        try:
            if ('tid' not in action_body) or  ('vid' not in action_body) or  ('imei' not in action_body) :
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['tid'] is None or action_body['tid'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['vid'] is None or action_body['vid'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['imei'] is None or action_body['imei'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            vid = action_body['vid'];
            tid = action_body['tid'];
            imei_str = action_body['imei'];
            imei = int(action_body['imei'])
            setparamdict = dict()
            if 'silence_start' in action_body and action_body['silence_start'] is not None:
                setparamdict['silence_start'] = action_body['silence_start']
            if 'silence_end' in action_body and action_body['silence_end'] is not None:
                setparamdict['silence_end'] = action_body['silence_end']
            if 'sleep_start' in action_body and action_body['sleep_start'] is not None:
                setparamdict['sleep_start'] = action_body['sleep_start']
            if 'sleep_end' in action_body and action_body['sleep_end'] is not None:
                setparamdict['sleep_end'] = action_body['sleep_end']
            if 'location_interval' in action_body and action_body['location_interval'] is not None:
                setparamdict['location_interval'] = action_body['location_interval']
            if 'viberation' in action_body and action_body['viberation'] is not None:
                setparamdict['alert_motor'] = action_body['viberation']
            if 'mute' in action_body and action_body['mute'] is not None:
                setparamdict['alert_ring'] = action_body['mute']


            userinfo = dict()
            imeikey = None


            imeiinfo = None
            searchkey = self.KEY_IMEI_ID % (imei_str,'*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist) == 0:            
                imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
                if imeiinfo == None:
                    logger.error("can't found imei in cache!!!")
                    retdict['error_code'] = self.ERRORCODE_IMEI_NOT_EXIST
                    return
                else:
                    imei_id = imeiinfo['_id'].__str__()
                    imeikey = self.KEY_IMEI_ID % (imei_str,imei_id)
                    for key in imeiinfo:
                        if key in ['_id','createtime']:
                            imeiinfo[key] = imeiinfo[key].__str__()

                    self._redis.hmset(imeikey, imeiinfo)
                    logger.error("get back from db imei:%s"%(imeikey))
            else:
                imeikey = resultlist[0]
                imeiinfo = self._redis.hgetall(imeikey)

            if vid != self.ADMIN_VID:
                searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
                resultlist = self._redis.keys(searchkey)
                if len(resultlist) == 0:
                    logger.error("No found user token %s" %(tid))
                    retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                    return

                userinfo = self._redis.hgetall(resultlist[0])
                member_id = userinfo['_id']
                if 'device' not in userinfo or userinfo['device'] is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                    return
                imeidict = eval(userinfo['device'])
                if imei_str not in imeidict:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                    return

                if imeiinfo['owner'] != member_id:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_OWNER
                    return
            
            if imeikey is not None:
                self._redis.hmset(imeikey, setparamdict)

            self.collect_gearinfo.update_one({'imei':imei},{'$set':setparamdict})
            sendbuflist = list()
            for key in setparamdict:
                if key == 'alert_ring':
                    sendbuflist.append('%s|%s' % ('mute',setparamdict[key]))
                elif key == 'alert_motor':
                    sendbuflist.append('%s|%s' % ('vib',setparamdict[key]))
                elif key == 'location_interval':
                    sendbuflist.append('%s|%s' % ('locint',setparamdict[key]))
                elif key == 'silence_start':
                    sendbuflist.append('%s|%s' % ('silencestart',setparamdict[key]))
                elif key == 'silence_end':
                    sendbuflist.append('%s|%s' % ('silenceend',setparamdict[key]))
                elif key == 'sleep_start':
                    sendbuflist.append('%s|%s' % ('sleepstart',setparamdict[key]))
                elif key == 'sleep_end':
                    sendbuflist.append('%s|%s' % ('sleepend',setparamdict[key]))


            senddict = dict()

            if 'protocol' in imeiinfo and imeiinfo['protocol'] == self.PROTOCOL_VERSION_2:
                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
                senddict['sendbuf'] = "SETPARAM:%s#"%(','.join(sendbuflist))
                senddict['encode'] = 'ascii'
                senddict['protocol'] = self.PROTOCOL_VERSION_2


            onlinekey = self.KEY_IMEI_ONLINE_FLAG % (imei_str)
            if self._redis.exists(onlinekey) is False:
                #机器不在线退出
                retdict['error_code'] = self.ERRORCODE_IMEI_STATE_OOS
                return

            #发送消息
            if 'mqtt_publish' in self._config:
                self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))


            retdict['error_code'] = self.ERRORCODE_OK
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


if __name__ == "__main__":
    fileobj = open("/opt/Keeprapid/Wishoney/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()

    thread_count = 1
    if _config is not None and 'gearcenter' in _config and _config['gearcenter'] is not None:
        if 'thread_count' in _config['gearcenter'] and _config['gearcenter']['thread_count'] is not None:
            thread_count = int(_config['gearcenter']['thread_count'])

    for i in xrange(0, thread_count):
        GearCenter = GearCenter(i)
        GearCenter.setDaemon(True)
        GearCenter.start()

    while 1:
        time.sleep(1)
