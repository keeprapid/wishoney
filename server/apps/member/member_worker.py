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


class MemberLogic(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
#        super(MemberLogic, self).__init__()
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("MemberLogic :running in __init__")

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
        self.recv_queue_name = "W:Queue:Member"
        if 'member' in _config:
            if 'Consumer_Queue_Name' in _config['member']:
                self.recv_queue_name = _config['member']['Consumer_Queue_Name']

#        self.mongoconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.mongoconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
        self.db = self.mongoconn.member
        self.collect_memberinfo = self.db.memberinfo
        self.collect_memberlog = self.db.memberlog
#        self.gearconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.gearconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.dbgear = self.gearconn.gearcenter
        self.collect_gearinfo = self.dbgear.gearinfo
#        self.loconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.loconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.locdb = self.loconn.location
        self.colgps = self.locdb.gpsinfo
        self.collbs = self.locdb.lbsinfo

    def calcpassword(self, password, verifycode):
        m0 = hashlib.md5(verifycode)
        logger.debug("m0 = %s" % m0.hexdigest())
        m1 = hashlib.md5(password + m0.hexdigest())
    #        print m1.hexdigest()
        logger.debug("m1 = %s" % m1.hexdigest())
        md5password = m1.hexdigest()
        return md5password

    def generator_tokenid(self, userid, timestr, verifycode):
        m0 = hashlib.md5(verifycode)
    #        print m0.hexdigest()
        m1 = hashlib.md5("%s%s%s" % (userid,timestr,m0.hexdigest()))
    #        print m1.hexdigest()
        token = m1.hexdigest()
        return token

    def run(self):
        logger.debug("Start MemberLogic pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
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
        logger.debug("::::::RetBuf--->errorcode = %s " % (message_resp_dict.get('error_code')))

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

        if action_cmd == 'member_register':
            self._proc_action_member_regist(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_register2':
            self._proc_action_member_register2(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_update':
            self._proc_action_member_update(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'login':
            self._proc_action_login(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'logout':
            self._proc_action_logout(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_info':
            self._proc_action_member_info(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'change_password':
            self._proc_action_change_password(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'getback_password':
            self._proc_action_getback_password(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_add_gear':
            self._proc_action_member_add_gear(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_query_gear':
            self._proc_action_member_query_gear(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_update_gear':
            self._proc_action_member_update_gear(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_gear_add_fence':
            self._proc_action_member_gear_add_fence(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_del_gear':
            self._proc_action_member_del_gear(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_gear_headimg_upload':
            self._proc_action_member_gear_headimg_upload(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_update_fence_notify':
            self._proc_action_member_update_fence_notify(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'gear_reset':
            self._proc_action_gear_reset(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'reset_password':
            self._proc_action_reset_password(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_gear_family':
            self._proc_action_member_gear_family(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'remove_family_member':
            self._proc_action_remove_family_member(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_add_gear2':
            self._proc_action_member_add_gear2(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_email':
            self._proc_action_member_email(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = '40000'

        return

    def _proc_action_member_regist(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'register', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'username'    : M
                        'pwd'     : M
                        'vid'     : M
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                }
        '''
        logger.debug(" into _proc_action_member_regist action_body:[%s]:%s"%(version,action_body))
        try:
            
            if ('username' not in action_body) or  ('pwd' not in action_body) or  ('vid' not in action_body) or ('source' not in action_body) or ('verifycode' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['pwd'] is None or action_body['pwd'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['username'] is None or action_body['username'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_INVALID
                return
            if action_body['source'] is None or action_body['source'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_SOURCE_INVALID
                return
            if action_body['verifycode'] is None or action_body['verifycode'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_VERIFYCODE_ERROR
                return

            vid = action_body['vid'];
            source = action_body['source']
            verifycode = action_body['verifycode']

            username = urllib.unquote(action_body['username'].encode('utf-8')).decode('utf-8')
            if self.collect_memberinfo.find_one({'username': username}):
                retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_ALREADY_EXIST
                return
            if source == 'mobile':
                if self.collect_memberinfo.find_one({'mobile':username}):
                    retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_ALREADY_EXIST
                    return
            #检查verifycode
            verikey = self.KEY_PHONE_VERIFYCODE % (username, verifycode)
            resultlist = self._redis.keys(verikey)
            if len(resultlist) == 0:
                retdict['error_code'] = self.ERRORCODE_MEMBER_VERIFYCODE_ERROR
                return

            self._redis.delete(verikey)


            lang = 'chs'
            if 'lang' in action_body and action_body['lang'] is not None:
                lang = action_body['lang']

            nickname = username
            if 'nickname' in action_body and action_body['nickname'] is not None:
                nickname = urllib.unquote(action_body['nickname'].encode('utf-8')).decode('utf-8')

            if 'email' in action_body and action_body['email'] is not None:
                email = action_body['email']
            else:
                email = username

            if 'mobile' in action_body and action_body['mobile'] is not None:
                mobile = action_body['mobile']
            else:
                mobile = ''

            if 'enable_notify' in action_body and action_body['enable_notify'] is not None:
                enable_notify = action_body['enable_notify']
            else:
                enable_notify = '1'

            registionid = ''
            if 'registionId' in action_body and action_body['registionId'] is not None:
                registionid = action_body['registionId']
            ios_apnstype = 'release'
            if 'ios_apnstype' in action_body and action_body['ios_apnstype'] is not None:
                ios_apnstype = action_body['ios_apnstype']

            userprofile = dict()
            userprofile['max_location_number'] = self.MAX_LOCATION_NUMBER
            userprofile['enable_notify'] = enable_notify
            userprofile['ios_token'] = list()
            userprofile['lang'] = lang

            insertmember = dict({\
                'username':username,\
                'nickname':nickname,\
                'source':source,\
                'password':self.calcpassword(action_body['pwd'],self.MEMBER_PASSWORD_VERIFY_CODE),\
                'vid':vid,\
                'createtime': datetime.datetime.now(),\
                'lastlogintime':None,\
                'state': 0,\
                'mobile':mobile,\
                'email':email,\
                'last_sms_time':dict(),\
                'last_callrecord_time':dict(),\
                'friends':list(),\
                'account':dict(),\
                'userprofile':userprofile,\
                'registionId':registionid,\
                'ios_apnstype':ios_apnstype,\
                'device':dict()\
                })
            logger.debug(insertmember)
            self.collect_memberinfo.insert_one(insertmember)

            retdict['error_code'] = self.ERRORCODE_OK
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_member_register2(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'register', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'username'    : M
                        'pwd'     : M
                        'vid'     : M
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                }
        '''
        logger.debug(" into _proc_action_member_regist2 action_body:[%s]:%s"%(version,action_body))
        try:
            
            if ('username' not in action_body) or  ('pwd' not in action_body) or  ('vid' not in action_body) or ('source' not in action_body) or ('nationalcode' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['pwd'] is None or action_body['pwd'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['username'] is None or action_body['username'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_INVALID
                return
            if action_body['source'] is None or action_body['source'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_SOURCE_INVALID
                return
            if action_body['nationalcode'] is None or action_body['nationalcode'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['vid'];
            source = action_body['source']
            nationalcode = action_body['nationalcode']

            if nationalcode == '86':
                username = urllib.unquote(action_body['username'].encode('utf-8')).decode('utf-8')
                username1= username[2:]
                if self.collect_memberinfo.find_one({'username': {'$in':[username,username1]}}):
                    retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_ALREADY_EXIST
                    return
                if source == 'mobile':
                    if self.collect_memberinfo.find_one({'mobile':{'$in':[username,username1]}}):
                        retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_ALREADY_EXIST
                        return

            else:
                username = urllib.unquote(action_body['username'].encode('utf-8')).decode('utf-8')
                if self.collect_memberinfo.find_one({'username': username}):
                    retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_ALREADY_EXIST
                    return
                if source == 'mobile':
                    if self.collect_memberinfo.find_one({'mobile':username}):
                        retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_ALREADY_EXIST
                        return
            #检查verifycode


            lang = 'chs'
            if 'lang' in action_body and action_body['lang'] is not None:
                lang = action_body['lang']

            nickname = username
            if 'nickname' in action_body and action_body['nickname'] is not None:
                nickname = urllib.unquote(action_body['nickname'].encode('utf-8')).decode('utf-8')

            if 'email' in action_body and action_body['email'] is not None:
                email = action_body['email']
            else:
                email = username

            if 'mobile' in action_body and action_body['mobile'] is not None:
                mobile = action_body['mobile']
            else:
                mobile = ''

            if 'enable_notify' in action_body and action_body['enable_notify'] is not None:
                enable_notify = action_body['enable_notify']
            else:
                enable_notify = '1'

            registionid = ''
            if 'registionId' in action_body and action_body['registionId'] is not None:
                registionid = action_body['registionId']
            ios_apnstype = 'release'
            if 'ios_apnstype' in action_body and action_body['ios_apnstype'] is not None:
                ios_apnstype = action_body['ios_apnstype']

            userprofile = dict()
            userprofile['max_location_number'] = self.MAX_LOCATION_NUMBER
            userprofile['enable_notify'] = enable_notify
            userprofile['ios_token'] = list()
            userprofile['lang'] = lang

            insertmember = dict({\
                'username':username,\
                'nickname':nickname,\
                'source':source,\
                'password':self.calcpassword(action_body['pwd'],self.MEMBER_PASSWORD_VERIFY_CODE),\
                'vid':vid,\
                'createtime': datetime.datetime.now(),\
                'lastlogintime':None,\
                'state': 0,\
                'mobile':mobile,\
                'email':email,\
                'last_sms_time':dict(),\
                'last_callrecord_time':dict(),\
                'friends':list(),\
                'account':dict(),\
                'userprofile':userprofile,\
                'nationalcode':nationalcode,\
                'registionId':registionid,\
                'ios_apnstype':ios_apnstype,\
                'device':dict()\
                })
            logger.debug(insertmember)
            self.collect_memberinfo.insert_one(insertmember)

            retdict['error_code'] = self.ERRORCODE_OK
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_login(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'login', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'username'    : M
                        'pwd'     : M
                        'vid'     : M
                        'phone_name':O
                        'phone_os':O
                        'app_version':O
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
        logger.debug(" into _proc_action_login action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('username' not in action_body) or  ('pwd' not in action_body) or  ('vid' not in action_body) or ('source' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['pwd'] is None or action_body['pwd'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['username'] is None or action_body['username'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_INVALID
                return

            nationalcode = '86'
            if 'nationalcode' in action_body and action_body['nationalcode'] is not None :
                nationalcode = action_body['nationalcode']

            username = urllib.unquote(action_body['username'].encode('utf-8')).decode('utf-8')
            vid = action_body['vid'];
            #在redis中查找用户登陆信息
            tokenid = None
            nickname = ""
            memberid = ''
            addkey = ''
            source = action_body['source']
            searchkey = self.KEY_TOKEN_NAME_ID % ('*',username,'*')
            logger.debug("key = %s" % searchkey)
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % resultlist)
            if len(resultlist):
                redis_memberinfo = self._redis.hgetall(resultlist[0])
                if redis_memberinfo is None:
                    #redis中没有用户信息，就从mongo中读取
                    member = self.collect_memberinfo.find_one({'username': username})
                    if member is None:
                        retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                        return
                    #比较密码
                    if action_body['pwd'] != member['password']:
                        retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                        return
                    #产生tokenid
                    memberid = member['_id'].__str__()
                    tokenid = self.generator_tokenid(memberid, str(datetime.datetime.now()), self.MEMBER_PASSWORD_VERIFY_CODE)
                    #写入redis
                    addkey = self.KEY_TOKEN_NAME_ID % (tokenid,member['username'], memberid)
                    self._redis.hset(addkey, 'tid', tokenid)
                    for key in member:
                        if key == '_id':
                            self._redis.hset(addkey, key, memberid)
                        elif key == 'tid':
                            continue
                        elif key in ['createtime', 'lastlogintime']:
                            self._redis.hset(addkey, key, member[key].__str__())
                        else:
                            self._redis.hset(addkey, key, member[key])
                    #删除无用的key
                    self._redis.delete(resultlist[0])
                    nickname = member.get('nickname')

                else:
                    memberid = redis_memberinfo['_id']
                    tokenid = redis_memberinfo['tid']
                    password = redis_memberinfo['password']
                    nickname = redis_memberinfo.get('nickname')
                    addkey = resultlist[0]
                    #比较密码
                    if action_body['pwd'] != password:
                        retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                        return
            else:
                member = None
                if nationalcode == '86' and username.startswith('86') == True:
                    usernameold = username[2:]
                    member = self.collect_memberinfo.find_one({'username':{'$in':[username,usernameold]}})
                else:
                    member = self.collect_memberinfo.find_one({'username': username})

                if member is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                    return
                #比较密码
                if action_body['pwd'] != member['password']:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                    return

                memberid = member['_id'].__str__()
                nickname = member.get('nickname')

                searchkey = self.KEY_TOKEN_NAME_ID % ('*', member['username'], member['_id'].__str__())
                resultlist = self._redis.keys(searchkey)
                if len(resultlist)>0:
                    memberinfo = self._redis.hgetall(resultlist[0])
                    tokenid = memberinfo.get('tid')
                else:
                #产生tokenid
                    tokenid = self.generator_tokenid(memberid, str(datetime.datetime.now()), self.MEMBER_PASSWORD_VERIFY_CODE)
                #写入redis
                    addkey = self.KEY_TOKEN_NAME_ID % (tokenid,member['username'], memberid)
                    self._redis.hset(addkey, 'tid', tokenid)
                    for key in member:
                        if key == '_id':
                            self._redis.hset(addkey, key, memberid)
                        elif key == 'tid':
                            continue
                        elif key in ['createtime', 'lastlogintime']:
                            self._redis.hset(addkey, key, member[key].__str__())
                        else:
                            self._redis.hset(addkey, key, member[key])


            #更新上次登陆时间
            t = datetime.datetime.now()
            registionId = ''
            if 'registionId' in action_body and action_body['registionId'] is not None:
                registionId = action_body['registionId']
            else:
                registionId = ''

            # debug
            ios_apnstype = 'release'
            if 'ios_apnstype' in action_body and action_body['ios_apnstype'] is not None:
                ios_apnstype = action_body['ios_apnstype']
            else:
                ios_apnstype = 'release'

            updateredistdict = dict()
            updatedict = dict()
            updateredistdict['lastlogintime'] = t.__str__()
            updateredistdict['registionId'] = registionId
            updateredistdict['ios_apnstype'] = ios_apnstype

            updatedict['lastlogintime'] = t
            updatedict['registionId'] = registionId
            updatedict['ios_apnstype'] = ios_apnstype


            self._redis.hmset(addkey, updateredistdict)
            self.collect_memberinfo.update_one({'_id':ObjectId(memberid)},{'$set':updatedict})


            if 'phone_name' in action_body and action_body['phone_name'] is not None:
                phone_name = urllib.unquote(action_body['phone_name'].encode('utf-8')).decode('utf-8')
            else:
                phone_name = ''
            if 'phone_os' in action_body and action_body['phone_os'] is not None:
                phone_os = urllib.unquote(action_body['phone_os'].encode('utf-8')).decode('utf-8')
            else:
                phone_os =  ''
            if 'app_version' in action_body and action_body['app_version'] is not None:
                app_version = action_body['app_version']
            else:
                app_version =  ''
            if 'phone_id' in action_body and action_body['phone_id'] is not None:
                phone_id = urllib.unquote(action_body['phone_id'].encode('utf-8')).decode('utf-8')
            else:
                phone_id =  ''
            #添加登陆记录
            insertlog = dict({\
                'mid':memberid,\
                'tid':tokenid,\
                'vid':vid,\
                'phone_id': phone_id,\
                'phone_name': phone_name,\
                'phone_os': phone_os,\
                'app_version': app_version,\
                'timestamp': datetime.datetime.now()\
                })
            logger.debug(insertlog)
            self.collect_memberlog.insert_one(insertlog)

            retdict['error_code'] = self.ERRORCODE_OK
            retbody['tid'] = tokenid
            if source == "web":
                if isinstance(nickname, unicode):
                    retbody['nickname'] = urllib.quote(nickname.encode('utf-8'))
                else:
                    retbody['nickname'] = urllib.quote(nickname)

            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_logout(self, version, action_body, retdict, retbody):
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
        logger.debug(" into _proc_action_logout action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            ios_token = None
            if 'ios_token' in action_body and action_body['ios_token'] is not None:
                ios_token = action_body['ios_token']

            tid = action_body['tid']
            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist):
                memberinfo = self._redis.hgetall(resultlist[0])
                self._redis.hset(resultlist[0],'registionId','')
                if 'userprofile' in memberinfo:
                    userprofile = eval(memberinfo.get('userprofile'))
                    if 'ios_token' in userprofile:
                        if ios_token is not None and ios_token in userprofile['ios_token']:
                            logger.debug("delete ios_tokenid")
                            userprofile['ios_token'].remove(ios_token)

                        if len(userprofile['ios_token']) >0:
                            logger.debug("update ios_tokenid")
                            self._redis.hset(resultlist[0],'userprofile',userprofile)

                        else:
                            self.redisdelete(resultlist)
                    else:
                        self.redisdelete(resultlist)
                else:
                    self.redisdelete(resultlist)

            retdict['error_code'] = '200'
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_add_gear(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei    M   设备的IMEI
                        vid M   厂商id
                        relationship    O   
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_add_gear action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or ('imei' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None or action_body['imei'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['tid']
            tid = action_body['tid']
            imei_str = action_body['imei']
            imei = int(imei_str)
            relationship = ''
            if 'relationship' in action_body and action_body['relationship'] is not None:
                relationship = urllib.unquote(action_body['relationship'].encode('utf-8')).decode('utf-8')

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            objectid_memberid = ObjectId(tni['_id'])
            memberid = tni['_id']

            memberinfo = self.collect_memberinfo.find_one({'_id':objectid_memberid})
            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return

            device = memberinfo['device']

            gearinfo = self.collect_gearinfo.find_one({'imei':imei})
            if gearinfo is None:
                retdict['error_code'] = self.ERRORCODE_IMEI_NOT_EXIST
                return

            follow = gearinfo['follow']

            if imei_str in device and memberid in follow:
                #已经互相绑定过，直接返回成功
                retdict['error_code'] = self.ERRORCODE_MEMBER_ALREADY_FOLLOW_GEAR
                return

            if len(follow) >= gearinfo['maxfollowcount']:
                retdict['error_code'] = self.ERRORCODE_IMEI_OUT_MAXCOUNT
                return

            follow_set = set(follow)
            follow_set.add(memberid)
            follow = list(follow_set)
            logger.debug(follow)

            gearsetinfo = dict()
            gearsetinfo['follow'] = follow
            role = '0'
            if gearinfo.get('owner') == None or gearinfo.get('owner') == "":
                role = '1'
                gearsetinfo['owner'] = memberid
                gearinfo['owner'] = memberid



            gearid = gearinfo['_id'].__str__()
            device[imei_str] = dict()
            device[imei_str]['imei'] = imei
            device[imei_str]['objectid'] = gearid
            device[imei_str]['relationship'] = relationship
            device[imei_str]['timestamp'] = datetime.datetime.now().__str__()
            device[imei_str]['role'] = role

            self.collect_memberinfo.update_one({'_id':objectid_memberid},{'$set':{'device':device}})
            self.collect_gearinfo.update_one({'imei':imei},{'$set':gearsetinfo})
#            self.collect_gearinfo.update_one({'imei':imei},{'$set':{'follow':follow}})

            gearkey = self.KEY_IMEI_ID % (imei_str, gearid)
            self._redis.hset(tnikey, 'device', device)
            #如果redis中有key，只更新部分数据，否则整体更新
            resultlist = self._redis.keys(gearkey)
            if len(resultlist):
                self._redis.hmset(gearkey, gearsetinfo)
            else:
                gearinfo['follow'] = follow
                for key in gearinfo:
                    if key == '_id':
                        continue
#                    elif key == 'nickname':
#                        self._redis.hset(gearkey, key, gearinfo[key])
                    elif key == 'follow':
                        if gearinfo[key] == None:
                            self._redis.hset(gearkey, key, '[]')
                        else:
                            self._redis.hset(gearkey, key, gearinfo[key])
                    elif key in ['createtime','activetime']:
                        self._redis.hset(gearkey, key, gearinfo[key].__str__())
                    else:
                        self._redis.hset(gearkey, key, gearinfo[key])

            retbody['gearinfo'] = dict()
            retbody['gearinfo']['relationship'] = urllib.quote(relationship.encode('utf-8'))
            retbody['gearinfo']['role'] = role
            for key in gearinfo:
                if key in ['follow','maxfollowcount','_id']:
                    continue
                elif key in ['createtime', 'activetime']:
                    retbody['gearinfo'][key] = gearinfo[key].__str__()
                elif key in ['nickname','img_url']:
                    retbody['gearinfo'][key] = urllib.quote(gearinfo[key].encode('utf-8'))
                else:
                    retbody['gearinfo'][key] = gearinfo[key]

            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_member_add_gear2(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei    M   设备的IMEI
                        vid M   厂商id
                        relationship    O   
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_add_gear2 action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or ('imei' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None or action_body['imei'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['tid']
            tid = action_body['tid']
            imei_str = action_body['imei']
            imei = int(imei_str)
            relationship = ''
            if 'relationship' in action_body and action_body['relationship'] is not None:
                relationship = urllib.unquote(action_body['relationship'].encode('utf-8')).decode('utf-8')

            ownerid = None
            if 'ownerid' in action_body and action_body['ownerid'] is not None:
                ownerid = action_body['ownerid']


            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            objectid_memberid = ObjectId(tni['_id'])
            memberid = tni['_id']

            memberinfo = self.collect_memberinfo.find_one({'_id':objectid_memberid})
            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return

            device = memberinfo['device']

            gearinfo = self.collect_gearinfo.find_one({'imei':imei})
            logger.debug(gearinfo)
            if gearinfo is None:
                retdict['error_code'] = self.ERRORCODE_IMEI_NOT_EXIST
                return

            follow = gearinfo['follow']

            if imei_str in device and memberid in follow:
                #已经互相绑定过，直接返回成功
                retdict['error_code'] = self.ERRORCODE_MEMBER_ALREADY_FOLLOW_GEAR
                return

            if len(follow) >= gearinfo['maxfollowcount']:
                retdict['error_code'] = self.ERRORCODE_IMEI_OUT_MAXCOUNT
                return

            follow_set = set(follow)
            follow_set.add(memberid)
            follow = list(follow_set)
            logger.debug(follow)

            gearsetinfo = dict()
            gearsetinfo['follow'] = follow
            role = '0'
            if gearinfo.get('owner') == None or gearinfo.get('owner') == "":
                role = '1'
                gearsetinfo['owner'] = memberid
                gearinfo['owner'] = memberid
            else:
                if ownerid is None or ownerid != gearinfo['owner']:
                    retdict['error_code'] = self.ERRORCODE_IMEI_HAS_OWNER
                    return                    

            gearid = gearinfo['_id'].__str__()
            device[imei_str] = dict()
            device[imei_str]['imei'] = imei
            device[imei_str]['objectid'] = gearid
            device[imei_str]['relationship'] = relationship
            device[imei_str]['timestamp'] = datetime.datetime.now().__str__()
            device[imei_str]['role'] = role

            self.collect_memberinfo.update_one({'_id':objectid_memberid},{'$set':{'device':device}})
            self.collect_gearinfo.update_one({'imei':imei},{'$set':gearsetinfo})
#            self.collect_gearinfo.update_one({'imei':imei},{'$set':{'follow':follow}})

            gearkey = self.KEY_IMEI_ID % (imei_str, gearid)
            self._redis.hset(tnikey, 'device', device)
            #如果redis中有key，只更新部分数据，否则整体更新
            resultlist = self._redis.keys(gearkey)
            if len(resultlist):
                self._redis.hmset(gearkey, gearsetinfo)
            else:
                gearinfo['follow'] = follow
                for key in gearinfo:
                    if key == '_id':
                        continue
#                    elif key == 'nickname':
#                        self._redis.hset(gearkey, key, gearinfo[key])
                    elif key == 'follow':
                        if gearinfo[key] == None:
                            self._redis.hset(gearkey, key, '[]')
                        else:
                            self._redis.hset(gearkey, key, gearinfo[key])
                    elif key in ['createtime','activetime']:
                        self._redis.hset(gearkey, key, gearinfo[key].__str__())
                    else:
                        self._redis.hset(gearkey, key, gearinfo[key])

            retbody['gearinfo'] = dict()
            retbody['gearinfo']['relationship'] = urllib.quote(relationship.encode('utf-8'))
            retbody['gearinfo']['role'] = role
            for key in gearinfo:
                if key in ['follow','maxfollowcount','_id']:
                    continue
                elif key in ['createtime', 'activetime']:
                    retbody['gearinfo'][key] = gearinfo[key].__str__()
                elif key in ['nickname','img_url']:
                    retbody['gearinfo'][key] = urllib.quote(gearinfo[key].encode('utf-8'))
                else:
                    retbody['gearinfo'][key] = gearinfo[key]

            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_info(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        vid M   厂商id
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_info action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['tid']
            tid = action_body['tid']

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
            for key in tni:
                if key in ['username', 'nickname']:
                    retbody[key] = urllib.quote(tni[key])
                elif key in ['_id', 'password', 'createtime']:
                    continue
                elif key in ['account', 'userprofile','last_sms_time','last_callrecord_time','device']:
                    retbody[key] = eval(tni[key])
                    if key == 'device':
                        for keyimei in retbody[key]:
                            if retbody[key][keyimei].get('relationship') is None:
                                retbody[key][keyimei]['relationship'] = ''

                else:
                    retbody[key] = tni[key]


            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_email(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        vid M   厂商id
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_email action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('username' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['username'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            nationalcode = '86'
            if 'nationalcode' in action_body and action_body['nationalcode'] is not None:
                nationalcode = action_body['nationalcode']

            username = urllib.unquote(action_body['username'].encode('utf-8')).decode('utf-8')
            vid = action_body['vid']
            memberinfo = None
            if nationalcode == '86' and username.startswith('86'):
                username1 = username[2:]
                memberinfo = self.collect_memberinfo.find_one({'username':{'$in':[username, username1]}})
            else:
                memberinfo = self.collect_memberinfo.find_one({'username':username})

            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return
            email = ""
            if 'email' in memberinfo:
                email = memberinfo['email']

            retdict['error_code'] = self.ERRORCODE_OK
            retbody['email'] = email

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_query_gear(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei O  
                        vid
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_query_gear action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['tid']
            tid = action_body['tid']

            imei_str = None
            imei = None
            if 'imei' in action_body and action_body['imei'] is not None:
                imei_str = action_body['imei']
                imei = int(imei_str)

            retbody['gearinfo'] = list()

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
                retdict['error_code'] = self.ERRORCODE_OK
                retbody['gearinfo'] = list()
                return
            deviceinfo = eval(tni['device'])
            logger.debug(deviceinfo)

            if imei_str is not None and imei is not None:
                #只查单条信息
                if imei_str not in deviceinfo:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                    retbody['gearinfo'] = list()
                    return
                else:
                    #先找内存里面的
                    searchkey = self.KEY_IMEI_ID % (imei_str, '*')
                    resultlist = self._redis.keys(searchkey)
                    if len(resultlist):
                        imeikey = resultlist[0]
                        imeiinfo = self._redis.hgetall(imeikey)
                        imeidict = dict()
                        imeidict['relationship'] = deviceinfo[imei_str].get('relationship')
                        imeidict['role'] = deviceinfo[imei_str].get('role')
                        for key in imeiinfo:
                            if key in ['follow','maxfollowcount', '_id']:
                                continue
                            elif key in ['fence_list']:
                                imeidict[key] = eval(imeiinfo[key])
                            elif key in ['nickname','img_url']:
                                imeidict[key] = urllib.quote(imeiinfo[key])
                            else:
                                imeidict[key] = imeiinfo[key]
                        retbody['gearinfo'].append(imeidict)
                        retdict['error_code'] = self.ERRORCODE_OK
                        return
                    else:
                        #否则找数据库的
                        imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
                        if imeiinfo:                           
                            imeidict = dict()
                            imeidict['relationship'] = deviceinfo[imei_str].get('relationship')
                            imeidict['role'] = deviceinfo[imei_str].get('role')
                            for key in imeiinfo:
                                if key in ['follow','maxfollowcount','_id']:
                                    continue
                                elif key in ['fence_list']:
                                    imeidict[key] = eval(imeiinfo[key])
                                elif key in ['createtime', 'activetime']:
                                    imeidict[key] = imeiinfo[key].__str__()
                                elif key in ['nickname','img_url']:
                                    imeidict[key] = urllib.quote(imeiinfo[key].encode('utf-8'))
                                elif key in ['imei']:
                                    imeidict[key] = str(imeiinfo[key])
                                else:
                                    imeidict[key] = imeiinfo[key]
                            retbody['gearinfo'].append(imeidict)
                            retdict['error_code'] = self.ERRORCODE_OK
                            return
                        else:
                            retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                            return
            else:
                for imei_str in deviceinfo:
                    #先找内存里面的
                    searchkey = self.KEY_IMEI_ID % (imei_str, '*')
                    resultlist = self._redis.keys(searchkey)
                    logger.debug(searchkey)
                    logger.debug(resultlist)
                    if len(resultlist):
                        imeikey = resultlist[0]
                        imeiinfo = self._redis.hgetall(imeikey)
                        imeidict = dict()
                        imeidict['relationship'] = deviceinfo[imei_str].get('relationship')
                        imeidict['role'] = deviceinfo[imei_str].get('role')
                        for key in imeiinfo:
                            if key in ['follow','maxfollowcount']:
                                continue
                            elif key in ['fence_list']:
                                imeidict[key] = eval(imeiinfo[key])
                            elif key in ['nickname','img_url']:
                                imeidict[key] = urllib.quote(imeiinfo[key])
                            else:
                                imeidict[key] = imeiinfo[key]
                        retbody['gearinfo'].append(imeidict)
                    else:
                        #否则找数据库的
                        imei = int(imei_str)
                        imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
                        if imeiinfo:                           
                            imeidict = dict()
                            imeidict['relationship'] = deviceinfo[imei_str].get('relationship')
                            imeidict['role'] = deviceinfo[imei_str].get('role')
                            for key in imeiinfo:
                                if key in ['follow','maxfollowcount','_id']:
                                    continue
                                elif key in ['fence_list']:
                                    imeidict[key] = imeiinfo[key]
                                elif key in ['createtime', 'activetime']:
                                    imeidict[key] = imeiinfo[key].__str__()
                                elif key in ['nickname','img_url']:
                                    imeidict[key] = urllib.quote(imeiinfo[key].encode('utf-8'))
                                elif key in ['imei']:
                                    imeidict[key] = str(imeiinfo[key])
                                else:
                                    imeidict[key] = imeiinfo[key]
                            retbody['gearinfo'].append(imeidict)

                retdict['error_code'] = self.ERRORCODE_OK
                return


            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_member_update(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_update', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        vid M   厂商id
                        email   O   
                        mobile  O   手机
                        nickname    O   nickname 用qutoe编码
                        ios_device_token    O   
                        enable_notify   O   0-no 1-yes
                        birthday        
                        lang
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_update action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['tid']
            tid = action_body['tid']

            updatedict = dict()
            userprofiledict = dict()
            userprofiledict['ios_token'] = list()
            last_sms_time = None;
            last_callrecord_time = None

            for key in action_body:
                if key == 'nickname':
                    updatedict[key] = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')
                elif key in ['enable_notify', 'lang']:
                    userprofiledict[key] = action_body[key]
                elif key in ['ios_token']:
                    userprofiledict['ios_token'].append(action_body[key])
                elif key in ['last_sms_time']:
                    last_sms_time = action_body[key]
                elif key in ['last_callrecord_time']:
                    last_callrecord_time = action_body[key]
                elif key in ['tid','vid']:
                    continue
                else:
                    updatedict[key] = action_body[key]

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return
            member_id = ObjectId(tni['_id'])
            for key in updatedict:
                self._redis.hset(tnikey, key, updatedict[key])

            if 'userprofile' in tni:
                old_userprofiledict = eval(tni['userprofile'])
                if 'ios_token' not in old_userprofiledict:
                   old_userprofiledict['ios_token'] = list()
            else:
                old_userprofiledict = dict()
                old_userprofiledict['ios_token'] = list()

            for key in userprofiledict:
                if key == 'ios_token':
                    old_userprofiledict['ios_token'] = list(set(old_userprofiledict['ios_token']).union(set(userprofiledict[key])))
                else:
                    old_userprofiledict[key] = userprofiledict[key]

            self._redis.hset(tnikey, 'userprofile', old_userprofiledict)

            if last_sms_time:
                tmplist = last_sms_time.split(';')
                if len(tmplist) == 2:
                    imei = tmplist[0]
                    timestamp = tmplist[1]
                    if 'last_sms_time' in tni:
                        lastdict = eval(tni.get('last_sms_time'))
                    else:
                        lastdict = dict()

                    lastdict[imei] = timestamp
                    self._redis.hset(tnikey,'last_sms_time', lastdict)
                    self.collect_memberinfo.update_one({'_id':member_id}, {'$set':{'last_sms_time':lastdict}})

            if last_callrecord_time:
                tmplist = last_callrecord_time.split(';')
                if len(tmplist) == 2:
                    imei = tmplist[0]
                    timestamp = tmplist[1]
                    if 'last_callrecord_time' in tni:
                        lastdict = eval(tni.get('last_callrecord_time'))
                    else:
                        lastdict = dict()

                    lastdict[imei] = timestamp
                    self._redis.hset(tnikey,'last_callrecord_time', lastdict)
                    self.collect_memberinfo.update_one({'_id':member_id}, {'$set':{'last_callrecord_time':lastdict}})


            if 'ios_token' in old_userprofiledict:
                old_userprofiledict.pop('ios_token')

            self.collect_memberinfo.update_one({'_id':member_id}, {'$set':updatedict})
            self.collect_memberinfo.update_one({'_id':member_id}, {'$set':{'userprofile':old_userprofiledict}})

            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_change_password(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'change_password', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                    tid M   tokenid
                    vid M   
                    old_password    M   用户密码（verifycode = “abcdef”, 
                                        先用m0 = md5(verifycode),
                                        再m1 = md5(password+m0)

                    new_password    M   用户密码（verifycode = “abcdef”, 
                                        先用m0 = md5(verifycode),
                                        再m1 = md5(password+m0)

                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_change_password action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or ('old_password' not in action_body) or  ('new_password' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None or action_body['old_password'] is None or action_body['new_password'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['tid']
            tid = action_body['tid']
            old_password = action_body['old_password']
            new_password = action_body['new_password']

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return
            member_id = ObjectId(tni['_id'])
            
            if 'password' not in tni or tni['password'] != old_password:
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return

            self._redis.hset(tnikey, 'password', new_password)    
            self.collect_memberinfo.update_one({'_id':member_id}, {'$set':{'password':new_password}})
            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL





    def _proc_action_getback_password(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'getback_password', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        username    M   用户名
                        vid M   
                        lang    O   当前版本的语言
                                    eng
                                    chs
                                    cht
                                    …

                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_getbackpassword action_body")
        try:
#        if 1:
            
            if ('username' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['username'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            nationalcode = '86'
            if 'nationalcode' in action_body and action_body['nationalcode'] is not None:
                nationalcode = action_body['nationalcode']

            lang = 'chs'
            if 'lang' in action_body and action_body['lang'] is not None:
                lang = action_body['lang']

            username = urllib.unquote(action_body['username'].encode('utf-8')).decode('utf-8')
            vid = action_body['vid']

            if nationalcode == '86' and username.startswith('86'):
                username1 = username[2:]
                memberinfo = self.collect_memberinfo.find_one({'username':{'$in':[username, username1]}})
            else:
                memberinfo = self.collect_memberinfo.find_one({'username':username})

            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return

            if 'email' not in memberinfo or memberinfo['email'] == "":
                retdict['error_code'] = self.ERRORCODE_MEMBER_NO_EMAIL
                return

            newpassword = "%d" % random.randint(100000,999999)
            md5password = self.calcpassword(newpassword, self.MEMBER_PASSWORD_VERIFY_CODE)
            logger.debug("newpassword = %s, md5password = %s" % (newpassword,md5password))
            #找对应的语言模板
            fileobj = open("/opt/Keeprapid/Wishoney/server/conf/notifytempelate.conf","r")
            templateinfo = json.load(fileobj)
            fileobj.close()

            getbackpassword_t = templateinfo[self.NOTIFY_TYPE_GETBACKPASSWORD]
            if lang not in getbackpassword_t:
                templateinfo = getbackpassword_t['eng']
            else:
                templateinfo = getbackpassword_t[lang]

            sendcontent = templateinfo['content'] % (newpassword)
            body = dict()
            body['dest'] = memberinfo['email']
            body['content'] = urllib.quote(sendcontent.encode('utf-8'))
            body['carrier'] = 'email'
            body['notify_type'] = self.NOTIFY_TYPE_GETBACKPASSWORD
            body['email_from'] = urllib.quote(templateinfo['email_from'].encode('utf-8'))
            body['email_subject'] = urllib.quote(templateinfo['email_subject'].encode('utf-8'))
            action = dict()
            action['body'] = body
            action['version'] = '1.0'
            action['action_cmd'] = 'send_email_notify'
            action['seq_id'] = '%d' % random.randint(0,10000)
            action['from'] = ''

#            sendcontent = templateinfo['content'] % (newpassword)
#            body = dict()
#            body['mobile'] = mobile
#            body['content'] = urllib.quote(sendcontent.encode('utf-8'))
#            action = dict()
#            action['body'] = body
#            action['version'] = '1.0'
#            action['action_cmd'] = 'send_sms'
#            action['seq_id'] = '%d' % random.randint(0,10000)
#            action['from'] = ''

            self._sendMessage(self._config['notify']['Consumer_Queue_Name'], json.dumps(action))

            #修改密码
            self.collect_memberinfo.update_one({'_id':memberinfo['_id']},{'$set':{'password':md5password}})
            #删除redis的cache数据
            searchkey = self.KEY_TOKEN_NAME_ID % ('*',username, memberinfo['_id'].__str__())
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                self.redisdelete(resultlist)

            retdict['error_code'] = self.ERRORCODE_OK
            return


        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_reset_password(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'getback_password', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        username    M   用户名
                        vid M   
                        lang    O   当前版本的语言
                                    eng
                                    chs
                                    cht
                                    …

                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_reset_password action_body")
        try:
#        if 1:
            
            if ('username' not in action_body) or  ('vid' not in action_body) or ('password' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['username'] is None or action_body['vid'] is None or action_body['password'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            nationalcode = '86'
            if 'nationalcode' in action_body and action_body['nationalcode'] is not None:
                nationalcode = action_body['nationalcode']

            lang = 'chs'
            if 'lang' in action_body and action_body['lang'] is not None:
                lang = action_body['lang']

            username = urllib.unquote(action_body['username'].encode('utf-8')).decode('utf-8')
            vid = action_body['vid']
            #app传递的password已经是md5加密过的数据
            password = action_body['password']

            if nationalcode == '86' and username.startswith('86'):
                username1 = username[2:]
                memberinfo = self.collect_memberinfo.find_one({'username':{'$in':[username, username1]}})
            else:
                memberinfo = self.collect_memberinfo.find_one({'username':username})

            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return

            #修改密码
            self.collect_memberinfo.update_one({'_id':memberinfo['_id']},{'$set':{'password':password}})
            #删除redis的cache数据
            searchkey = self.KEY_TOKEN_NAME_ID % ('*',memberinfo['username'], memberinfo['_id'].__str__())
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                self.redisdelete(resultlist)

            retdict['error_code'] = self.ERRORCODE_OK
            return


        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_update_gear(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_update_gear', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei    M   用户id
                        vid M   厂商id
                        mobile  O   设备手机号
                        name    O   用户名
                        gender  O   用户性别
                        height  O   cm
                        weight  O   kg
                        bloodtype   O   
                        stride  O   
                        birth   O   ‘2014-09-23’
                        relationship    O   
                        alarm_enable    O   
                        alarm_center_lat    O   警戒围栏中心纬度
                        alarm_center_long   O   警戒围栏中心经度
                        alarm_center_radius O   警戒围栏半径（米）
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_update_gear action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or  ('imei' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None or action_body['imei'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['tid']
            tid = action_body['tid']
            imei_str = action_body['imei']
            imei = int(imei_str)

            updategeardict = dict()
            relationship = None
            for key in action_body:
                if key in ['tid','vid','imei']:
                    continue
                elif key == 'nickname':
                    updategeardict[key] = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')
                elif key == 'relationship':
                    if action_body[key] is None:
                        relationship = ""
                    else:
                        relationship = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')
                else:
                    updategeardict[key] = action_body[key]

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return
            object_member_id = ObjectId(tni['_id'])
            memberid = tni['_id']

            searchkey = self.KEY_IMEI_ID % (imei_str, '*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_IMEI_CACHE_ERROR
                return
            imeikey = resultlist[0]
            imeiinfo = self._redis.hgetall(imeikey)

            #检查member是否有添加过imei
            if 'device' not in tni or tni['device'] is None or tni['device'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                return

            device = eval(tni['device'])
            if imei_str not in device:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                return

            #检查gear是否被member关注
            if 'follow' not in imeiinfo or imeiinfo['follow'] is None or imeiinfo['follow'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                return

            follow = eval(imeiinfo['follow'])
            if memberid not in follow:
                retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                return

                
            device[imei_str]['relationship'] = relationship
            self._redis.hset(tnikey , 'device', device)
            self.collect_memberinfo.update_one({'_id':object_member_id},{'$set':{'device':device}})

            logger.debug(updategeardict)
            for key in updategeardict:
                self._redis.hset(imeikey, key, updategeardict[key])

            if len(updategeardict):
                self.collect_gearinfo.update_one({'imei':imei},{'$set':updategeardict})

            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_remove_family_member(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_del_gear', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei    M   用户id
                        vid M   厂商id

                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_remove_family_member action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or  ('imei' not in action_body) or  ('remove_memberid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None or action_body['imei'] is None or action_body['remove_memberid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['tid']
            tid = action_body['tid']
            imei_str = action_body['imei']
            imei = int(imei_str)

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return
            object_owner_id = ObjectId(tni['_id'])
            owner_id = tni['_id']

            remove_memberid = action_body['remove_memberid']
            object_remove_memberid = ObjectId(remove_memberid)
            removememberinfo = self.collect_memberinfo.find_one({'_id':object_remove_memberid})
            searchkey = self.KEY_TOKEN_NAME_ID % ('*', '*', remove_memberid)
            resultlist = self._redis.keys(searchkey)
            removetnikey = None
            if len(resultlist)>0:
                removetnikey = resultlist[0]

            searchkey = self.KEY_IMEI_ID % (imei_str, '*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_IMEI_CACHE_ERROR
                return
            imeikey = resultlist[0]
            imeiinfo = self._redis.hgetall(imeikey)

            #检查是否是owner
            if owner_id != imeiinfo['owner']:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_OWNER
                return
            #检查member是否有添加过imei
#            if 'device' not in removememberinfo or removememberinfo['device'] is None or removememberinfo['device'] == '':
#                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
#                return
#            #检查gear是否被member关注
#            if 'follow' not in imeiinfo or imeiinfo['follow'] is None or imeiinfo['follow'] == '':
#                retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
#                return

            device = removememberinfo['device']
            if imei_str in device:
                device.pop(imei_str)


            follow = eval(imeiinfo['follow'])
            if remove_memberid in follow:
                follow.remove(remove_memberid)

            if removetnikey is not None:
                self._redis.hset(removetnikey , 'device', device)

            self._redis.hset(imeikey, 'follow', follow)

            self.collect_memberinfo.update_one({'_id':object_remove_memberid},{'$set':{'device':device}})

            self.collect_gearinfo.update_one({'imei':imei},{'$set':{'follow':follow}})

            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_del_gear(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_del_gear', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei    M   用户id
                        vid M   厂商id

                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_del_gear action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or  ('imei' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None or action_body['imei'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['tid']
            tid = action_body['tid']
            imei_str = action_body['imei']
            imei = int(imei_str)

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return
            object_member_id = ObjectId(tni['_id'])
            memberid = tni['_id']

            searchkey = self.KEY_IMEI_ID % (imei_str, '*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_IMEI_CACHE_ERROR
                return
            imeikey = resultlist[0]
            imeiinfo = self._redis.hgetall(imeikey)

            #检查member是否有添加过imei
            if 'device' not in tni or tni['device'] is None or tni['device'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                return
            #检查gear是否被member关注
            if 'follow' not in imeiinfo or imeiinfo['follow'] is None or imeiinfo['follow'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                return

            device = eval(tni['device'])
            if imei_str in device:
                device.pop(imei_str)


            follow = eval(imeiinfo['follow'])
            if memberid in follow:
                follow.remove(memberid)


            self._redis.hset(tnikey , 'device', device)
            self._redis.hset(imeikey, 'follow', follow)

            self.collect_memberinfo.update_one({'_id':object_member_id},{'$set':{'device':device}})

            self.collect_gearinfo.update_one({'imei':imei},{'$set':{'follow':follow}})

            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_gear_headimg_upload(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_del_gear', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei    M   用户id
                        vid M   厂商id

                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_gear_headimg_upload action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or  ('imei' not in action_body) or  ('img' not in action_body) or  ('format' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None or action_body['imei'] is None or action_body['format'] is None or action_body['img'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['vid']
            tid = action_body['tid']
            imei_str = action_body['imei']
            imei = int(imei_str)

            format = action_body['format']
            img = action_body['img']

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return
            object_member_id = ObjectId(tni['_id'])
            memberid = tni['_id']

            searchkey = self.KEY_IMEI_ID % (imei_str, '*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_IMEI_CACHE_ERROR
                return
            imeikey = resultlist[0]
            imeiinfo = self._redis.hgetall(imeikey)

            #检查member是否有添加过imei
            if 'device' not in tni or tni['device'] is None or tni['device'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                return
            #检查gear是否被member关注
            if 'follow' not in imeiinfo or imeiinfo['follow'] is None or imeiinfo['follow'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                return

            #找到user，处理上传的头像
            fileobj = open("/opt/Keeprapid/Wishoney/server/conf/upload.conf",'r')
            uploadconfig = json.load(fileobj)
            fileobj.close()

            filedir = "%s%s" % (uploadconfig['FILEDIR_IMG_HEAD'], vid)
            filename = "%s.%s" % (imei_str, format)
            imgurl = "%s/%s/%s" % (uploadconfig['imageurl'],vid,filename)
            logger.debug(filedir)
            logger.debug(filename)
            logger.debug(imgurl)

            if os.path.isdir(filedir):
                pass
            else:
                os.mkdir(filedir)

            filename = "%s/%s" % (filedir, filename)
            fs = open(filename, 'wb')
            fs.write(base64.b64decode(img))
            fs.flush()
            fs.close()

            retbody['img_url'] = imgurl

            self._redis.hset(imeikey, 'img_url', imgurl)
            self.collect_gearinfo.update_one({'imei':imei},{'$set':{'img_url':imgurl}})

            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_gear_add_fence(self, version, action_body, retdict, retbody):
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
        logger.debug(" into _proc_action_member_gear_add_fence action_body:%s"%action_body)
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
            if ('fence_name' not in action_body) or action_body['fence_name'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if ('coord_list' not in action_body) or action_body['coord_list'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            fence_direction = self.FENCE_DIRECTION_OUT
            if 'fence_direction' in action_body and action_body['fence_direction'] is not None:
                fence_direction = action_body['fence_direction']

            fence_enable = 1
            if 'fence_enable' in action_body and action_body['fence_enable'] is not None:
                fence_enable = int(action_body['fence_enable'])


            vid = action_body['vid'];
            tid = action_body['tid'];
            imei_str = action_body['imei'];
            imei = int(action_body['imei'])
            fence_name = urllib.unquote(action_body['fence_name'].encode('utf-8')).decode('utf-8')
            coord_list = action_body['coord_list']
            logger.debug(coord_list)

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return
            object_member_id = ObjectId(tni['_id'])
            memberid = tni['_id']

            searchkey = self.KEY_IMEI_ID % (imei_str, '*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_IMEI_CACHE_ERROR
                return
            imeikey = resultlist[0]
            imeiinfo = self._redis.hgetall(imeikey)

            if vid != self.ADMIN_VID:
                #检查member是否有添加过imei
                if 'device' not in tni or tni['device'] is None or tni['device'] == '':
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                    return

                device = eval(tni['device'])
                if imei_str not in device:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                    return

                #检查gear是否被member关注
                if 'follow' not in imeiinfo or imeiinfo['follow'] is None or imeiinfo['follow'] == '':
                    retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                    return

                follow = eval(imeiinfo['follow'])
                if memberid not in follow:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                    return

            #检查围栏信息
            if 'fence_list' in imeiinfo:
                oldfencelist = eval(imeiinfo['fence_list'])
            else:
                oldfencelist = dict()

            fenceinfo = dict()
            oldfencelist[fence_name] = fenceinfo
            fenceinfo['fence_direction'] = fence_direction
            fenceinfo['fence_enable'] = fence_enable
            fenceinfo['coord_list'] = coord_list

            self._redis.hset(imeikey,'fence_list',oldfencelist)
            self.collect_gearinfo.update_one({'imei':imei},{'$set':{'fence_list':oldfencelist}})
            retdict['error_code'] = self.ERRORCODE_OK
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_update_fence_notify(self, version, action_body, retdict, retbody):
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
        logger.debug(" into _proc_action_member_update_fence_notify action_body:%s"%action_body)
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
            if ('enable_fence' not in action_body) or action_body['enable_fence'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return


            vid = action_body['vid'];
            tid = action_body['tid'];
            imei_str = action_body['imei'];
            imei = int(action_body['imei'])
            enable_fence = action_body['enable_fence']

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return
            object_member_id = ObjectId(tni['_id'])
            memberid = tni['_id']


            if 'device' not in tni or tni['device'] is None or tni['device'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                return

            device = eval(tni['device'])
            if imei_str not in device:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                return

            imeiinfo = device[imei_str]
            imeiinfo['enable_fence'] = enable_fence

            self._redis.hset(tnikey,'device',device)
            self.collect_memberinfo.update_one({'_id':ObjectId(memberid)},{'$set':{'device':device}})
            retdict['error_code'] = self.ERRORCODE_OK
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_gear_reset(self, version, action_body, retdict, retbody):
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
        logger.debug(" into _proc_action_gear_reset action_body:%s"%action_body)
        try:
            if 'imeilist' not in action_body:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['imeilist'] is None or action_body['imeilist'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            imeilist = action_body['imeilist'].split(',');

            for imei in imeilist:
                imei_int = int(imei)
                resetdict = dict()
                resetdict['last_long'] = 0
                resetdict['alert_motor'] = 0
                resetdict['last_lat'] = 0
                resetdict['last_location_time'] = ''
                resetdict['height'] = '0'
                resetdict['follow'] = list()
                resetdict['alarm_lat'] = 0
                resetdict['weight'] = '0'
                resetdict['nickname'] = ''
                resetdict['alert_ring'] = '0'
                resetdict['fence_list'] = dict()
                resetdict['last_location_objectid'] = ''
                resetdict['last_location_type'] = '0'
                resetdict['img_url'] = ''
                resetdict['friend_number'] = ''
                resetdict['battery_level'] = '0'
                resetdict['last_angle'] = '0'
                resetdict['alarm_long'] = 0
                resetdict['monitor_number'] = ''
                resetdict['last_vt'] = '0'
                resetdict['birth'] = ''
                resetdict['last_angel'] = 0
                resetdict['sos_number'] = ''
                resetdict['alarm_radius'] = 0
                resetdict['last_at'] = '0'
                resetdict['mobile'] = ''
                resetdict['last_lbs_objectid'] = ''
                resetdict['alarm_enable'] = 1
                resetdict['owner'] = ''


                gearinfo = self.collect_gearinfo.find_one({'imei':imei_int})
                if gearinfo != None:
                    follow = gearinfo['follow']
                    for memberid in follow:
                        memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
                        deviceinfo = memberinfo['device']
                        if imei in deviceinfo:
                            deviceinfo.pop(imei)

                        self.collect_memberinfo.update_one({'_id':ObjectId(memberid)},{'$set':{'device':deviceinfo}})
                        membersearchkey = self.KEY_TOKEN_NAME_ID % ('*','*',memberid)
                        result = self._redis.keys(membersearchkey)
                        if len(result) > 0:
                            memberkey = result[0]
                            self._redis.hset(memberkey, 'device', deviceinfo)
                self.collect_gearinfo.update_one({'imei':imei_int},{'$set':resetdict})
                searchkey = self.KEY_IMEI_ID % (imei, '*')
                result = self._redis.keys(searchkey)
                if len(result) >0:
                    gearkey = result[0]
                    self._redis.hmset(gearkey, resetdict)

                self.colgps.delete_one({'imei':imei_int})
                self.collbs.delete_one({'imei':imei_int})
                #清除数据
                senddict = dict()
                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei)
                senddict['sendbuf'] = "generalcmd,SOS,,,,MONITOR,,,FRIEND,,,,,,#"

                #先缓存消息到redis，等待回应或者等待下次发送
                cmdkey = self.KEY_IMEI_CMD % (imei, self.CMD_TYPE_SET_NUMBER)
                self._redis.hmset(cmdkey, senddict)
                self._redis.hset(cmdkey, 'soslist', ',,')
                self._redis.hset(cmdkey, 'monitorlist', ',')
                self._redis.hset(cmdkey, 'friendlist', ',,,,')
                self._redis.hset(cmdkey,'tid','')
                self._redis.expire(cmdkey,self.UNSEND_MSG_TIMEOUT)

                #发送消息
                if 'mqtt_publish' in self._config:
                    self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))

            retdict['error_code'] = self.ERRORCODE_OK
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_member_gear_family(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei O  
                        vid
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_gear_family action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or ('imei' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None or action_body['imei'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['tid']
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

            searchkey = self.KEY_IMEI_ID % (imei_str,'*')
            resultlist = self._redis.keys(searchkey)
            memberlist = list()
            retbody['memberlist'] = memberlist

            if len(resultlist)>0:
                gearinfo = self._redis.hgetall(resultlist[0])
                retbody['owner'] = gearinfo['owner']
                follow = eval(gearinfo['follow'])
                for memberid in follow:
                    memberinfo = dict()
                    skey = self.KEY_TOKEN_NAME_ID % ('*','*',memberid)
                    rlist = self._redis.keys(skey)
                    if len(rlist)>0:
                        tniinfo = self._redis.hgetall(rlist[0])
#                        logger.debug(tniinfo)
                        memberinfo['_id'] = tniinfo['_id']
                        memberinfo['nickname'] = urllib.quote(tniinfo['nickname'])
                        memberinfo['username'] = tniinfo['username']
                        devicedict = eval(tniinfo['device'])
                        if imei_str in devicedict:
                            relationship = devicedict[imei_str]['relationship'];
                            if relationship is None:
                                relationship = ""
                            memberinfo['relationship'] = urllib.quote(relationship)
                            memberinfo['role'] = devicedict[imei_str]['role']
                        memberlist.append(memberinfo)
                    else:
                        tniinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
#                        logger.debug(tniinfo)
                        memberinfo['_id'] = tniinfo['_id'].__str__()
                        memberinfo['nickname'] = urllib.quote(tniinfo['nickname'].encode('utf-8'))
                        memberinfo['username'] = tniinfo['username']
                        devicedict = tniinfo['device']
                        if imei_str in devicedict:
                            relationship = devicedict[imei_str]['relationship'];
                            if relationship is None:
                                relationship = ""
                            memberinfo['relationship'] = urllib.quote(relationship)
                            memberinfo['role'] = devicedict[imei_str]['role']
                        memberlist.append(memberinfo)


                retdict['error_code'] = self.ERRORCODE_OK
                return


            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

if __name__ == "__main__":
    fileobj = open("/opt/Keeprapid/Wishoney/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()

    thread_count = 1
    if _config is not None and 'member' in _config and _config['member'] is not None:
        if 'thread_count' in _config['member'] and _config['member']['thread_count'] is not None:
            thread_count = int(_config['member']['thread_count'])

    for i in xrange(0, thread_count):
        memberlogic = MemberLogic(i)
        memberlogic.setDaemon(True)
        memberlogic.start()

    while 1:
        time.sleep(1)
