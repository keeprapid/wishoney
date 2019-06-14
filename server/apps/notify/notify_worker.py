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

import xmltodict

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


class NotifyCenter(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
#        super(NotifyCenter, self).__init__()
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("NotifyCenter :running in __init__")

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
        self.recv_queue_name = "W:Queue:Member"
        if 'notify' in _config:
            if 'Consumer_Queue_Name' in _config['notify']:
                self.recv_queue_name = _config['notify']['Consumer_Queue_Name']
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
#        self.mongoconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.mongoconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.db = self.mongoconn.notify
        self.notify_log = self.db.notify_log
        self.smspassword = self.calcpassword(self.SMS_Password, self.SMS_Verifycode)
#        self.memberconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.memberconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.memberdb = self.memberconn.member
        self.collect_memberinfo = self.memberdb.memberinfo


    def calcpassword(self, password, verifycode):
        m0 = hashlib.md5(verifycode)
        logger.debug("m0 = %s" % m0.hexdigest())
        m1 = hashlib.md5(password + m0.hexdigest())
    #        print m1.hexdigest()
        logger.debug("m1 = %s" % m1.hexdigest())
        md5password = m1.hexdigest()
        return md5password


    def run(self):
        logger.debug("Start NotifyCenter pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
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

        if action_cmd == 'send_email_notify':
            self._proc_action_send_email_notify(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'send_notify':
            self._proc_action_send_notify(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'send_verifycode':
            self._proc_action_send_verifycode(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'send_sms':
            self._proc_action_send_sms(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = '40000'

        return

    def _sendEmail(self, dest, content, email_from, email_subject):
        fs = open("/opt/Keeprapid/Wishoney/server/conf/email.conf", "r")
        gwconfig = json.load(fs)
        fs.close()
        logger.debug(gwconfig)

        msg = MIMEText(content, _subtype=gwconfig['content_type'], _charset=gwconfig['content_charset'])
        msg['Subject'] = email_subject.encode(gwconfig['content_charset'])
        msg['From'] = email_from.encode(gwconfig['content_charset'])
        msg['To'] = dest
        try:
            #注意端口号是465，因为是SSL连接
            s = smtplib.SMTP()
            emailhost = gwconfig['gwip']
            logger.debug(emailhost)
            s.connect(emailhost)
            s.login(gwconfig['from'],gwconfig['password'])
#            print gwconfig['email']['email_from'].encode(gwconfig['email']['email_charset'])
            s.sendmail(email_from.encode(gwconfig['content_charset']), dest.split(';'), msg.as_string())
            time.sleep(self.SEND_EMAIL_INTERVAL)
            s.close()
            return '200',None
        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            return '400',None      


    def _proc_action_send_email_notify(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'gear_add', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'to'    : M
                        'content'     : M
                        'carrier'     : M
                        'notify_type'   : M
                        
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                }
        '''
        logger.debug(" into _proc_action_send_notify action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('dest' not in action_body) or  ('content' not in action_body) or  ('carrier' not in action_body) or  ('notify_type' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['dest'] is None or action_body['content'] is None or action_body['carrier'] is None or action_body['notify_type'] is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return

            dest = action_body['dest']
            carrier = action_body['carrier']
            notify_type = action_body['notify_type']
            content = urllib.unquote(action_body['content'].encode('utf-8')).decode('utf-8')
            if carrier == 'email':
                if 'email_from' in action_body and action_body['email_from'] is not None:
                    email_from = urllib.unquote(action_body['email_from'].encode('utf-8')).decode('utf-8')
                else:
                    email_from = 'noreply@keeprapid.com'

                if 'email_subject' in action_body and action_body['email_subject'] is not None:
                    email_subject = urllib.unquote(action_body['email_subject'].encode('utf-8')).decode('utf-8')
                else:
                    email_subject = 'From keeprapid'

                self._sendEmail(dest, content, email_from, email_subject)

            logdict = dict()
            logdict['from'] = email_from
            logdict['to'] = dest
            logdict['notify_type'] = notify_type
            logdict['carrier'] = carrier
            logdict['content'] = content
            logdict['timestamp'] = datetime.datetime.now()
            self.notify_log.insert_one(logdict)
                
            retdict['error_code'] = self.ERRORCODE_OK
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_send_sms(self, version, action_body, retdict, retbody):
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
        SMS_UserName = 'Wishoney'
        SMS_Password = '123456'
        SMS_Verifycode = 'abcdef'
        SMS_URL = 'http://120.24.238.189:8080/hawk_center/utils/smsagent.php?action=sendSms&account=%s&password=%s&extsubcode=866&phoneNumber=%s&content=%s'
        '''
        logger.debug(" into _proc_action_send_sms action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('mobile' not in action_body) or ('content') not in action_body:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['mobile'] is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['content'] is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return

            mobile = action_body['mobile']
            sendcontent1 = action_body['content']

            url = self.SMS_URL % (self.SMS_UserName, self.smspassword,mobile,sendcontent1)
            logger.debug(url)
            respones = urllib.urlopen(url)
            ret = xmltodict.parse(respones.read())

            if ret.get('result').get('errorCode') == '200':
                retdict['error_code'] = '200'
                logdict = dict()
                logdict['to'] = mobile
                logdict['notify_type'] = 'SMS'
                logdict['carrier'] = 'Clousky/Hawk'
                logdict['content'] = urllib.unquote(sendcontent1.encode('utf-8')).decode('utf-8')
                logdict['timestamp'] = datetime.datetime.now()
                self.notify_log.insert_one(logdict)
            else:
                retdict['error_code'] = self.ERRORCODE_MEMBER_VERIFYCODE_FAILEDBY_GATEWAY

            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_send_verifycode(self, version, action_body, retdict, retbody):
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
        SMS_UserName = 'Wishoney'
        SMS_Password = '123456'
        SMS_Verifycode = 'abcdef'
        SMS_URL = 'http://120.24.238.189:8080/hawk_center/utils/smsagent.php?action=sendSms&account=%s&password=%s&extsubcode=866&phoneNumber=%s&content=%s'
        '''
        logger.debug(" into _proc_action_send_verifycode action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('mobile' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['mobile'] is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return

            mobile = action_body['mobile']
            if self.collect_memberinfo.find_one({'username': mobile}):
                retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_ALREADY_EXIST
                return
            searchkey = self.KEY_PHONE_VERIFYCODE % (mobile,'*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                retdict['error_code'] = self.ERRORCODE_MEMBER_VERIFYCODE_ALREADY_INUSE
                return
            lang = 'chs'
            if 'lang' in action_body and action_body['lang'] is not None:
                lang = action_body['lang']

            vcode = '%.6d' % (random.randint(0,100000))
            logger.debug(vcode)

            fileobj = open("/opt/Keeprapid/Wishoney/server/conf/notifytempelate.conf","r")
            templateinfo = json.load(fileobj)
            fileobj.close()

            sendvc_t = templateinfo[self.NOTIFY_TYPE_SENDVC]
            if lang not in sendvc_t:
                t = sendvc_t['chs']
            else:
                t = sendvc_t[lang]
        
            sendcontent = t['content'] % (vcode)
            logger.debug(sendcontent)

            sendcontent1 = urllib.quote(sendcontent.encode('utf-8'))

            url = self.SMS_URL % (self.SMS_UserName, self.smspassword,mobile,sendcontent1)
            logger.debug(url)
            respones = urllib.urlopen(url)
            ret = xmltodict.parse(respones.read())

            if ret.get('result').get('errorCode') == '200':
                key = self.KEY_PHONE_VERIFYCODE % (mobile, vcode)
                self._redis.set(key, vcode)
                self._redis.expire(key, 5*60)
                retdict['error_code'] = '200'
                logdict = dict()
                logdict['to'] = mobile
                logdict['notify_type'] = 'SMS'
                logdict['carrier'] = 'Clousky/Hawk'
                logdict['content'] = sendcontent
                logdict['timestamp'] = datetime.datetime.now()
                self.notify_log.insert_one(logdict)
            else:
                retdict['error_code'] = self.ERRORCODE_MEMBER_VERIFYCODE_FAILEDBY_GATEWAY

            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_send_notify(self, version, action_body, retdict, retbody):
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
        logger.debug(" into _proc_action_send_notify action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('imei' not in action_body) or  ('notify_id' not in action_body) or  ('imeikey' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['imei'] is None or action_body['notify_id'] is None or action_body['imeikey'] is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return

            imei_str = action_body['imei']
            if isinstance(imei_str,str) == False:
                imei_str = str(imei_str)

            imei = int(imei_str)
            notify_id = action_body['notify_id']
            imeikey = action_body['imeikey']
            if 'tid' in action_body and action_body['tid'] is not None:
                tid = action_body['tid']
                searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
                resultlist = self._redis.keys(searchkey)
                if len(resultlist):
                    memberkey = resultlist[0]
                    memberinfo = self._redis.hgetall(memberkey)
                    self.notify2App(memberinfo, memberkey, notify_id, imei_str)
            else:
                imeiinfo = self._redis.hgetall(imeikey)
                if imeiinfo:
                    if 'follow' in imeiinfo and imeiinfo['follow'] is not None:
                        follow = eval(imeiinfo['follow'])
                        logger.debug(follow)
                        if len(follow):
                            for memberid in follow:
                                searchkey = self.KEY_TOKEN_NAME_ID % ('*','*',memberid)
                                resultlist = self._redis.keys(searchkey)
                                if len(resultlist):
                                    logger.debug(resultlist)
                                    memberkey = resultlist[0]
                                    memberinfo = self._redis.hgetall(memberkey)
                                    self.notify2App(memberinfo, memberkey, notify_id, imei_str)


            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def notify2App(self, memberinfo, memberkey, notify_id, imei):
#        logger.debug(memberinfo)
#        logger.debug(notify_id)
        userprofile = dict()
        if 'userprofile' in memberinfo:
            userprofile = eval(memberinfo['userprofile'])

        if 'device' in memberinfo:
            device = eval(memberinfo['device'])
            if notify_id in [self.NOTIFY_CODE_LEAVE_FENCE, self.NOTIFY_CODE_INTO_FENCE]:
                if imei not in device or 'enable_fence' not in device[imei] or device[imei]['enable_fence'] == "0":
                    logger.debug("==do not send fence notify")
                    return
#            if 'enable_notify' in userprofile and userprofile['enable_notify'] == '1':
#            if 1:
                #通知android客户端
            androidkey = self.KEY_CLIENT_ONLINE_FLAG % (memberinfo.get('username'))
            
            if self._redis.exists(androidkey):
                logger.debug(androidkey)
                action = dict()
                action['topic'] = "%s%s"%(self._mqttconfig['mqtt_android_topic_prefix'],memberinfo.get('username'))
                action['sendbuf'] = "notify:%s"%(notify_id)
                if 'mqtt_publish' in self._config:
                    self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(action))
            #通知ios客户端：
            if 'vid' in memberinfo and memberinfo['vid'] == self.VID_SMALLC and 'registionId' in memberinfo and memberinfo['registionId'] != '':
                if 'jpushproxy' in self._config:
                    action = dict()
                    sendbody = dict()
                    action['action_cmd'] = 'send_jpush_notify'
                    action['version'] = '1.0'
                    action['seq_id'] = random.randint(0,9999999)
                    action['from'] = ""
                    action['body'] = sendbody
                    sendbody['registionId'] = memberinfo['registionId']
                    if 'ios_apnstype' in memberinfo:
                        sendbody['ios_apnstype'] = memberinfo['ios_apnstype']
                    else:
                        sendbody['ios_apnstype'] = "release"
                    sendbody['memberkey'] = memberkey
                    sendbody['content_type'] = 'dict'
                    sendbody['content'] = dict({'loc-key':"ANPS_%s"%(notify_id)})
                    self._sendMessage(self._config['jpushproxy']['Consumer_Queue_Name'], json.dumps(action))
            elif 'ios_token' in userprofile and userprofile['ios_token'] != '' and userprofile['ios_token'] is not None:
                ios_tokenlist = userprofile['ios_token']
                if len(ios_tokenlist):
#                        fileobj = open("/opt/Keeprapid/Wishoney/server/conf/notifytempelate.conf","r")
#                        templateinfo = json.load(fileobj)
#                        fileobj.close()
#                        if notify_id in templateinfo:
#                            lang = 'eng'
#                            content = ''
#                            if 'lang' in userprofile:
#                                lang = userprofile['lang']
#                            if lang in templateinfo[notify_id]:
#                                content = templateinfo[notify_id][lang]['content']
#                            else:
#                                content = templateinfo[notify_id]['chs']['content']
                    tokenlist = list()
                    for token in ios_tokenlist:
                        tokenlist.append(token)
                    logger.debug(tokenlist)
                    if len(tokenlist):
                        if 'anps_proxy' in self._config:
                            action = dict()
                            sendbody = dict()
                            action['action_cmd'] = 'send_anps_notify'
                            action['version'] = '1.0'
                            action['seq_id'] = random.randint(0,9999999)
                            action['from'] = ""
                            action['body'] = sendbody
                            sendbody['ios_token'] = tokenlist
                            sendbody['memberkey'] = memberkey
                            sendbody['content_type'] = 'dict'
                            sendbody['content'] = dict({'loc-key':"ANPS_%s"%(notify_id)})
                            if 'vid' in memberinfo and memberinfo['vid'] == self.VID_ODA:
                                self._sendMessage(self._config['apns_oda']['Consumer_Queue_Name'], json.dumps(action))
                            else:
                                self._sendMessage(self._config['anps_proxy']['Consumer_Queue_Name'], json.dumps(action))



if __name__ == "__main__":
    fileobj = open("/opt/Keeprapid/Wishoney/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()

    thread_count = 1
    if _config is not None and 'notify' in _config and _config['notify'] is not None:
        if 'thread_count' in _config['notify'] and _config['notify']['thread_count'] is not None:
            thread_count = int(_config['notify']['thread_count'])

    for i in xrange(0, thread_count):
        notifycenter = NotifyCenter(i)
        notifycenter.setDaemon(True)
        notifycenter.start()

    while 1:
        time.sleep(1)
