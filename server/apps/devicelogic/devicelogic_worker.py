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
import threading
import os
import logging
import logging.config
import datetime
import pymongo
import random
import urllib

logging.config.fileConfig("/opt/Keeprapid/Wishoney/server/conf/log.conf")
logger = logging.getLogger('wishoney')
if '/opt/Keeprapid/Wishoney/server/apps/common' not in sys.path:
    sys.path.append('/opt/Keeprapid/Wishoney/server/apps/common')
import workers



class DeviceLogic(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("DeviceLogic :running in __init__")

        fileobj = open('/opt/Keeprapid/Wishoney/server/conf/db.conf', 'r')
        self._json_dbcfg = json.load(fileobj)
        fileobj.close()

        fileobj = open("/opt/Keeprapid/Wishoney/server/conf/config.conf", "r")
        self._config = json.load(fileobj)
        fileobj.close()

        fileobj = open("/opt/Keeprapid/Wishoney/server/conf/mqtt.conf", "r")
        self._mqttconfig = json.load(fileobj)
        fileobj.close()

        self.thread_index = thread_index
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
        self.recv_queue_name = "W:Queue:DeviceLogic"
        if 'devicelogic' in _config:
            if 'Consumer_Queue_Name' in _config['devicelogic']:
                self.recv_queue_name = _config['devicelogic']['Consumer_Queue_Name']

        self.publish_queue_name = "W:Queue:MQTTPub"
        if 'mqtt_publish' in _config:
            if 'Consumer_Queue_Name' in _config['mqtt_publish']:
                self.publish_queue_name = _config['mqtt_publish']['Consumer_Queue_Name']

#        self.mongoconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.mongoconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.db = self.mongoconn.location
        self.collect_gpsinfo = self.db.gpsinfo
        self.collect_lbsinfo = self.db.lbsinfo

#        self.gearconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.gearconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.geardb = self.gearconn.gearcenter
        self.collect_gearinfo = self.geardb.gearinfo

#        self.informationconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.informationconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.informationdb = self.informationconn.message
        self.collect_message = self.informationdb.message
        self.collect_callrecord = self.informationdb.callrecord
        self.collect_gearclock = self.informationdb.gearclock

        self.infoconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.infodb = self.infoconn.info
        self.collect_infotemp = self.infodb.infotemp
        self.collect_infostate = self.infodb.infostate
        self.collect_infolog = self.infodb.infolog

    def run(self):
        logger.debug("Start DeviceLogic pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
        
        while 1:
            try:
                recvdata = self._redis.brpop(self.recv_queue_name)
                t1 = time.time()
                if recvdata:
                    self._proc_message(recvdata[1])
                logger.debug("_proc_message cost %f" % (time.time()-t1))                    
            except Exception as e:
                logger.debug("%s except raised : %s " % (e.__class__, e.args))

    def send_to_publish_queue(self, sendbuf):
        '''sendbuf : dict'''
        self._redis.lpush(self.publish_queue_name, json.dumps(sendbuf))

    def _proc_message(self, recvbuf):
#        logger.debug(recvbuf)
        cmdlist = recvbuf.split(':')
        if len(cmdlist) < 2:
            logger.error("message format error")
            return
        cmdname = cmdlist.pop(0)
        cmdbody = ":".join(cmdlist)
        cmdbody = cmdbody.replace('#','')

        if cmdname == 'register':
            self._proc_register(cmdbody)
        elif cmdname == 'steps':
            self._proc_steps(cmdbody)
        elif cmdname == 'location':
            self._proc_location(cmdbody)
        elif cmdname == 'location2':
            self._proc_location2(cmdbody)
        elif cmdname == 'time':
            self._proc_time(cmdbody)
        elif cmdname == 'profile':
            self._proc_profile(cmdbody)
        elif cmdname == 'quit':
            self._proc_quit(cmdbody)
        elif cmdname == 'generalcmd':
            self._proc_generalcmd(cmdbody)
        elif cmdname == 'appregist':
            self._proc_appregist(cmdbody)
#for 2.0 watch interface
        elif cmdname == 'register2':
            self._proc_register2(cmdbody)
        elif cmdname == 'GPS':
            self._proc_gps(cmdbody)
        elif cmdname == 'LBS':
            self._proc_lbs(cmdbody)
        elif cmdname == 'SETNBR':
            self._proc_setnbr(cmdbody)
        elif cmdname == 'GETNUMBER':
            self._proc_getnumber(cmdbody)
        elif cmdname == 'CALLLOG':
            self._proc_calllog(cmdbody)
        elif cmdname == 'KEEPALIVE':
            self._proc_keepalive(cmdbody)
        elif cmdname == 'AL':
            self._proc_alert(cmdbody)
        elif cmdname == 'INFOREQ':
            self._proc_inforeq(cmdbody)
        elif cmdname == 'INFODEC':
            self._proc_infodec(cmdbody)
        else:
            logger.error("UNKNOWN MESSAGE")

    def set_online_flag(self, imei):
        setkey = self.KEY_IMEI_ONLINE_FLAG % (imei)
        if self._redis.exists(setkey) is False:
            #发现是重新登陆时，交给session模块去处理未发送的消息
            body = dict()
            body['imei'] = imei
            action = dict()
            action['body'] = body
            action['version'] = '1.0'
            action['action_cmd'] = 'check_undo_msg'
            action['seq_id'] = '%d' % random.randint(0,10000)
            action['from'] = ''
            if 'session' in self._config:
                self._sendMessage(self._config['session']['Consumer_Queue_Name'], json.dumps(action))

        self._redis.set(setkey, datetime.datetime.now().__str__())
        self._redis.expire(setkey, self.GEAR_ONLINE_TIMEOUT)

    def set_online_flag2(self, imei, interval):
        setkey = self.KEY_IMEI_ONLINE_FLAG % (imei)
        if self._redis.exists(setkey) is False:
            #发现是重新登陆时，交给session模块去处理未发送的消息
            body = dict()
            body['imei'] = imei
            action = dict()
            action['body'] = body
            action['version'] = '1.0'
            action['action_cmd'] = 'check_undo_msg'
            action['seq_id'] = '%d' % random.randint(0,10000)
            action['from'] = ''
            if 'session' in self._config:
                self._sendMessage(self._config['session']['Consumer_Queue_Name'], json.dumps(action))

        self._redis.set(setkey, datetime.datetime.now().__str__())
        self._redis.expire(setkey, interval)


    def _proc_register(self, cmdbody):
        logger.debug("into _proc_register : %s" % (cmdbody))
        if cmdbody is '' or cmdbody == '':
            logger.error("cmdbody error")
            return

        cmdlist = cmdbody.split(',')
        if len(cmdlist) != 2:
            logger.error("cmdbody error")
            return

        imei_str = cmdlist[0]
        searchkey = self.KEY_IMEI_ID % (imei_str, '*')
        resultlist = self._redis.keys(searchkey)
        if len(resultlist) == 0:
            imei = int(imei_str)
            imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
            if imeiinfo == None:
                logger.error("can't found imei in cache!!!")
                return
            else:
                imei_id = imeiinfo['_id'].__str__()
                addkey = self.KEY_IMEI_ID % (imei_str,imei_id)
                for key in imeiinfo:
                    if key in ['_id','createtime']:
                        imeiinfo[key] = imeiinfo[key].__str__()

                self._redis.hmset(addkey, imeiinfo)
                logger.error("get back from db imei:%s"%(addkey))

        self.set_online_flag(imei_str)

        #回送消息
        senddict = dict()
        senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
        senddict['sendbuf'] = "register:0"
        self.send_to_publish_queue(senddict)


    def _proc_steps(self, cmdbody):
        logger.debug("into _proc_steps : %s" % (cmdbody))

    def _proc_location(self, cmdbody):
        logger.debug("into _proc_location : %s" % (cmdbody))
        if cmdbody is '' or cmdbody == '':
            logger.error("cmdbody error")
            return

        cmdlist = cmdbody.split(',')
        if len(cmdlist) != 18:
            logger.error("cmdbody error")
            return

        imei_str = cmdlist[0]
        imei = int(imei_str)
        searchkey = self.KEY_IMEI_ID % (imei_str, '*')
        resultlist = self._redis.keys(searchkey)
        if len(resultlist) == 0:
            imei = int(imei_str)
            imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
            if imeiinfo == None:
                logger.error("can't found imei in cache!!!")
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

        self.set_online_flag(imei_str)
        battery_level       = cmdlist[1]
        gsm_signal          = cmdlist[2]
        mcc                 = cmdlist[3]
        mnc                 = cmdlist[4]
        lac                 = cmdlist[5]
        cellid              = cmdlist[6]
        gpsop               = cmdlist[7]
        timet               = cmdlist[8]
        longitude           = cmdlist[9]
        latitude            = cmdlist[10]
        altitude            = cmdlist[11]
        angle               = cmdlist[12]
        vt                  = cmdlist[13]
        wifimac1            = cmdlist[14]
        wifisignal1         = cmdlist[15]
        wifimac2            = cmdlist[16]
        wifisignal2         = cmdlist[17]
        location_type       = self.LOCATION_TYPE_GPS
        timestamp           = datetime.datetime.now()

        longitude_f         = float(longitude[:-1])
        latitude_f          = float(latitude[:-1])

        insertdict = dict()
        insertdict['lbs'] = list()
        lbsinfo = dict()
        lbsinfo['mnc'] = mnc
        lbsinfo['mcc'] = mcc
        lbsinfo['lac'] = lac
        lbsinfo['cellid'] = cellid
        insertdict['lbs'].append(lbsinfo)
        insertdict['imei'] = imei
        insertdict['gpsop'] = gpsop
        insertdict['timet'] = timet
        insertdict['battery_level'] = battery_level
        insertdict['longitude'] = longitude
        insertdict['latitude'] = latitude
        insertdict['altitude'] = altitude
        insertdict['vt'] = vt
        insertdict['location_type'] = location_type
        insertdict['timestamp'] = timestamp
        insertdict['angle'] = angle

        insertdict['wifi'] = list()
        wifiinfo1 = dict()
        wifiinfo1['wifimac'] = wifimac1
        wifiinfo1['wifisignal'] = wifisignal1
        wifiinfo2 = dict()
        wifiinfo2['wifimac'] = wifimac2
        wifiinfo2['wifisignal'] = wifisignal2

        insertobj = self.collect_gpsinfo.insert_one(insertdict)

        #更新到gearinfo中去
        if insertobj:
            updatedict = dict()
            updatedict['battery_level'] = battery_level
            updatedict['gsm_level'] = gsm_signal
            updatedict['last_long'] = longitude
            updatedict['last_lat'] = latitude
            updatedict['last_at'] = altitude
            updatedict['last_vt'] = vt
            updatedict['last_angle'] = angle
            updatedict['last_location_objectid'] = insertobj.inserted_id.__str__()
            updatedict['last_location_type'] = self.LOCATION_TYPE_GPS
            updatedict['last_location_time'] = timestamp.__str__()

            self._redis.hmset(imeikey, updatedict)
            self.collect_gearinfo.update_one({'imei':imei},{'$set':updatedict})
        #查看是否需要围栏服务
        if 'alarm_enable' in imeiinfo and imeiinfo['alarm_enable'] == '1':
            body = dict()
            body['imei'] = imei
            body['latitude'] = latitude
            body['longitude'] = longitude
            body['imeikey'] = imeikey
            action = dict()
            action['body'] = body
            action['version'] = '1.0'
            action['action_cmd'] = 'check_location'
            action['seq_id'] = '%d' % random.randint(0,10000)
            action['from'] = ''
            if 'session' in self._config:
                self._sendMessage(self._config['session']['Consumer_Queue_Name'], json.dumps(action))
        #检查电量是否需要报警 发送报警信息给app
        if int(battery_level)<self.BATTERY_LEVEL_ALERT:
            if 'notify' in self._config:
                body = dict()
                body['imei'] = imei
                body['imeikey'] = imeikey
                body['notify_id'] = self.NOTIFY_CODE_LOW_BATTERY
                action = dict()
                action['body'] = body
                action['version'] = '1.0'
                action['action_cmd'] = 'send_notify'
                action['seq_id'] = '%d' % random.randint(0,10000)
                action['from'] = ''          
                self._sendMessage(self._config['notify']['Consumer_Queue_Name'], json.dumps(action))            

        #检查是否有紧急定位标志如果有就删除，然后通知用户定位成功
        locationkey = self.KEY_IMEI_CMD % (imei_str, self.CMD_TYPE_START_LOCATION)
        if self._redis.exists(locationkey):
            cmdinfo = self._redis.hgetall(locationkey)
            if 'notify' in self._config:
                body = dict()
                body['imei'] = imei
                body['imeikey'] = imeikey
                if 'tid' in cmdinfo:
                    body['tid'] = cmdinfo.get('tid')
                body['notify_id'] = self.NOTIFY_CODE_LOCATION_OK
                action = dict()
                action['body'] = body
                action['version'] = '1.0'
                action['action_cmd'] = 'send_notify'
                action['seq_id'] = '%d' % random.randint(0,10000)
                action['from'] = ''          
                self._sendMessage(self._config['notify']['Consumer_Queue_Name'], json.dumps(action))            
            self._redis.delete(locationkey)
            

    def _proc_location2(self, cmdbody):
        logger.debug("into _proc_location2 : %s" % (cmdbody))
        if cmdbody is '' or cmdbody == '':
            logger.error("cmdbody error")
            return

        cmdlist = cmdbody.split(',')
        #判断imei合法性
        imei_str = cmdlist[0]
        imei = int(imei_str)
        searchkey = self.KEY_IMEI_ID % (imei_str, '*')
        resultlist = self._redis.keys(searchkey)
        if len(resultlist) == 0:
            imei = int(imei_str)
            imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
            if imeiinfo == None:
                logger.error("can't found imei in cache!!!")
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

        self.set_online_flag(imei_str)

        #解码
        battery_level       = cmdlist[1]
        lbs_count           = cmdlist[2]        
        mcc                 = cmdlist[3]
        mnc                 = cmdlist[4]
        current_idx         = 4
        lbslist = list()
        for i in xrange(0,int(lbs_count)):
            current_idx+= 1
#            print current_idx
#            print cmdlist[current_idx]
            tmplist = cmdlist[current_idx].split('|')
            lbsinfo = dict()
            lbsinfo['lac'] = tmplist[0]
            lbsinfo['cellid'] = tmplist[1]
            lbsinfo['gsm_signal'] = tmplist[2]
            lbslist.append(lbsinfo)

        wifimac1            = cmdlist[current_idx+1]
        wifisignal1         = cmdlist[current_idx+2]
        wifimac2            = cmdlist[current_idx+3]
        wifisignal2         = cmdlist[current_idx+4]
        current_gsm_signal  = cmdlist[current_idx+5]
        timet               = cmdlist[current_idx+6]

        location_type       = self.LOCATION_TYPE_LBS
        timestamp           = datetime.datetime.now()
        #存数据库
        insertdict = dict()
        lbsinfolist = list()
        insertdict['lbs'] = lbsinfolist
        for lbs in lbslist:
            insertlbsinfo = dict()
            insertlbsinfo['mnc'] = mnc
            insertlbsinfo['mcc'] = mcc
            insertlbsinfo['lac'] = lbs['lac']
            insertlbsinfo['cellid'] = lbs['cellid']
            insertlbsinfo['gsm_signal'] = lbs['gsm_signal']
            lbsinfolist.append(insertlbsinfo)

        insertdict['timet'] = timet
        insertdict['location_type'] = location_type
        insertdict['timestamp'] = timestamp
        insertdict['battery_level'] = battery_level
        insertdict['gsm_level'] = current_gsm_signal
        wifiinfolist = list()
        insertdict['wifi'] = wifiinfolist
        wifiinfo1 = dict()
        wifiinfo1['wifimac'] = wifimac1
        wifiinfo1['wifisignal'] = wifisignal1
        wifiinfolist.append(wifiinfo1)
        wifiinfo2 = dict()
        wifiinfo2['wifimac'] = wifimac2
        wifiinfo2['wifisignal'] = wifisignal2
        wifiinfolist.append(wifiinfo2)
        insertdict['imei'] = imei
        insertobj = self.collect_lbsinfo.insert_one(insertdict)

        #更新到gearinfo中去
        if insertobj:
            updatedict = dict()
            updatedict['battery_level'] = battery_level
            updatedict['gsm_level'] = current_gsm_signal
            updatedict['last_lbs_objectid'] = insertobj.inserted_id.__str__()

            self._redis.hmset(imeikey, updatedict)
            self.collect_gearinfo.update_one({'imei':imei},{'$set':updatedict})
        #发送基站wifi定位解析的请求：
            body = dict()
            body['imei'] = imei
            body['imeikey'] = imeikey
            body['last_lbs_objectid'] = imeiinfo.get('last_lbs_objectid')
            body['lbs_objectid'] = insertobj.inserted_id.__str__()
            body['lbs'] = lbsinfolist
            body['wifi'] = wifiinfolist

            action = dict()
            action['body'] = body
            action['version'] = '1.0'
            action['action_cmd'] = 'lbs_location'
            action['seq_id'] = '%d' % random.randint(0,10000)
            action['from'] = ''
            if mcc == "460":
                if 'lbs_proxy' in self._config:
                    self._sendMessage(self._config['lbs_proxy']['Consumer_Queue_Name'], json.dumps(action))
            # else:
                # self._sendMessage('W:Queue:LbsGoogleProxy', json.dumps(action))
                
        #检查电量是否需要报警 发送报警信息给app
        if int(battery_level)<self.BATTERY_LEVEL_ALERT:
            if 'notify' in self._config:
                body = dict()
                body['imei'] = imei
                body['imeikey'] = imeikey
                body['notify_id'] = self.NOTIFY_CODE_LOW_BATTERY
                action = dict()
                action['body'] = body
                action['version'] = '1.0'
                action['action_cmd'] = 'send_notify'
                action['seq_id'] = '%d' % random.randint(0,10000)
                action['from'] = ''          
                self._sendMessage(self._config['notify']['Consumer_Queue_Name'], json.dumps(action))            
        #检查是否有紧急定位标志如果有就删除，然后通知用户定位成功
        locationkey = self.KEY_IMEI_CMD % (imei_str, self.CMD_TYPE_START_LOCATION)
        if self._redis.exists(locationkey):
            cmdinfo = self._redis.hgetall(locationkey)
            if 'notify' in self._config:
                body = dict()
                body['imei'] = imei
                body['imeikey'] = imeikey
                if 'tid' in cmdinfo:
                    body['tid'] = cmdinfo.get('tid')
                body['notify_id'] = self.NOTIFY_CODE_LOCATION_OK
                action = dict()
                action['body'] = body
                action['version'] = '1.0'
                action['action_cmd'] = 'send_notify'
                action['seq_id'] = '%d' % random.randint(0,10000)
                action['from'] = ''          
                self._sendMessage(self._config['notify']['Consumer_Queue_Name'], json.dumps(action))            

            self._redis.delete(locationkey)


    def _proc_time(self, cmdbody):
        logger.debug("into _proc_time : %s" % (cmdbody))

    def _proc_profile(self, cmdbody):
        logger.debug("into _proc_profile : %s" % (cmdbody))

    def _proc_quit(self, cmdbody):
        logger.debug("into _proc_quit : %s" % (cmdbody))

    def _proc_generalcmd(self, cmdbody):
#        logger.debug("into _proc_generalcmd : %s" % (cmdbody))
        if cmdbody is '' or cmdbody == '':
            logger.error("cmdbody error")
            return

        cmdlist = cmdbody.split(',')
        #判断imei合法性
        imei_str = cmdlist[0]
        imei = int(imei_str)
        searchkey = self.KEY_IMEI_ID % (imei_str, '*')
        resultlist = self._redis.keys(searchkey)
        if len(resultlist) == 0:
            imei = int(imei_str)
            imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
            if imeiinfo == None:
                logger.error("can't found imei in cache!!!")
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

        self.set_online_flag(imei_str)

        #解码
        cmd = cmdlist[1]
        cmd = cmd.replace('#','')
        logger.debug("cmd = %s" % (cmd))
        if cmd in ['SMS','sms']:
            logger.debug("into _proc_generalcmd : SMS %s" % (imei_str))
            cg = cmdlist[2]
            content = cmdlist[3][:-1]
            if 'protocol' in imeiinfo and imeiinfo['protocol'] == self.PROTOCOL_VERSION_2:
                content = content.decode('utf-8')
            else:
                content = content.decode('utf-16-le')
            logger.debug(content)
            insertsms = dict()
            insertsms['imei'] = imei
            insertsms['vid'] = imeiinfo['vid']
            insertsms['from'] = cg
            insertsms['type'] = self.MSG_TYPE_SMS
            insertsms['content'] = content
            insertsms['timestamp'] = datetime.datetime.now()
            self.collect_message.insert_one(insertsms)
            if 'notify' in self._config:
                body = dict()
                body['imei'] = imei
                body['imeikey'] = imeikey
                body['notify_id'] = self.NOTIFY_CODE_NEW_SMS
                action = dict()
                action['body'] = body
                action['version'] = '1.0'
                action['action_cmd'] = 'send_notify'
                action['seq_id'] = '%d' % random.randint(0,10000)
                action['from'] = ''          
                self._sendMessage(self._config['notify']['Consumer_Queue_Name'], json.dumps(action))            
        elif cmd in ['SOSDIAL','sosdial']:
            logger.debug("into _proc_generalcmd : %s" % (cmdbody))
            celled = cmdlist[2]
            answer = 0
            if len(cmdlist[3])>1:
                answer = int(cmdlist[3][:-1])
            else:
                answer = int(cmdlist[3])
                
            insertcallrecord = dict()
            insertcallrecord['imei'] = imei
            insertcallrecord['vid'] = imeiinfo['vid']
            insertcallrecord['number'] = celled
            insertcallrecord['direction'] = self.CALL_DIRECTION_WATCH_OUTGOING
            insertcallrecord['answer'] = answer
            insertcallrecord['timestamp'] = datetime.datetime.now()
            self.collect_callrecord.insert_one(insertcallrecord)
            if answer == self.CALL_UNCONNECT:
                if 'notify' in self._config:
                    #启动通知服务
                    body = dict()
                    body['imei'] = imei
                    body['imeikey'] = imeikey
                    body['notify_id'] = self.NOTIFY_CODE_SOS_UNCONNECT
                    action = dict()
                    action['body'] = body
                    action['version'] = '1.0'
                    action['action_cmd'] = 'send_notify'
                    action['seq_id'] = '%d' % random.randint(0,10000)
                    action['from'] = ''
                    self._sendMessage(self._config['notify']['Consumer_Queue_Name'], json.dumps(action))
        elif cmd in ['ALARM', 'alarm']:
            logger.debug("into _proc_generalcmd : %s" % (cmdbody))
            cmdkey = self.KEY_IMEI_CMD % (imei_str, self.CMD_TYPE_SET_CLOCK)
            if self._redis.exists(cmdkey):
                cmdinfo = self._redis.hgetall(cmdkey)
                insertdict = dict()
                insertdict['imei'] = imei
                insertdict['firedate'] = cmdinfo.get('firedate')
                insertdict['content'] = cmdinfo.get('content')
                insertdict['vid'] = imeiinfo.get('vid')
                insertdict['memberid'] = cmdinfo.get('memberid')
                self.collect_gearclock.insert_one(insertdict)
                self._redis.delete(cmdkey)
                #通知到用户
                if 'notify' in self._config:
                    body = dict()
                    body['imei'] = imei
                    body['imeikey'] = imeikey
                    if 'tid' in cmdinfo:
                        body['tid'] = cmdinfo['tid']
                    body['notify_id'] = self.NOTIFY_CODE_SET_CLOCK_OK
                    action = dict()
                    action['body'] = body
                    action['version'] = '1.0'
                    action['action_cmd'] = 'send_notify'
                    action['seq_id'] = '%d' % random.randint(0,10000)
                    action['from'] = ''
                    self._sendMessage(self._config['notify']['Consumer_Queue_Name'], json.dumps(action))
        elif cmd in ['SOS','sos']:
            logger.debug("into _proc_generalcmd : %s" % (cmdbody))
            cmdkey = self.KEY_IMEI_CMD % (imei_str, self.CMD_TYPE_SET_NUMBER)
            if self._redis.exists(cmdkey):
                cmdinfo = self._redis.hgetall(cmdkey)
                setdict = dict()
                if 'soslist' in cmdinfo:
                    setdict['sos_number'] = cmdinfo.get('soslist')
                if 'monitorlist' in cmdinfo:
                    setdict['monitor_number'] = cmdinfo.get('monitorlist')
                if 'friendlist' in cmdinfo:
                    setdict['friend_number'] = cmdinfo.get('friendlist')
                
                self._redis.hmset(imeikey, setdict)
                self.collect_gearinfo.update_one({'imei':imei},{'$set':setdict})
                self._redis.delete(cmdkey)
                #通知到用户
                if 'notify' in self._config:
                    body = dict()
                    body['imei'] = imei
                    body['imeikey'] = imeikey
                    if 'tid' in cmdinfo:
                        body['tid'] = cmdinfo['tid']
                    body['notify_id'] = self.NOTIFY_CODE_SET_SOS_OK
                    action = dict()
                    action['body'] = body
                    action['version'] = '1.0'
                    action['action_cmd'] = 'send_notify'
                    action['seq_id'] = '%d' % random.randint(0,10000)
                    action['from'] = ''          
                    self._sendMessage(self._config['notify']['Consumer_Queue_Name'], json.dumps(action))



    def _proc_appregist(self, cmdbody):
        logger.debug("into _proc_appregist : %s" % (cmdbody))
        if cmdbody is '' or cmdbody == '':
            logger.error("cmdbody error")
            return

        cmdlist = cmdbody.split(',')
        mobile = cmdlist[0]
        interval = int(cmdlist[1])
        clientonlinekey = self.KEY_CLIENT_ONLINE_FLAG % (mobile)
#        resultlist = self._redis.exists(clientonlinekey)
        if self._redis.exists(clientonlinekey) is False:
            #如果appclient 是之前超时的话就发送缓存消息，尚不实现
            pass

        self._redis.set(clientonlinekey,datetime.datetime.now().__str__())
        self._redis.expire(clientonlinekey, interval+10)


    def _proc_register2(self, cmdbody):
        logger.debug("into _proc_register2 : %s" % (cmdbody))
        if cmdbody is '' or cmdbody == '':
            logger.error("cmdbody error")
            return

        cmdlist = cmdbody.split(',')
        if len(cmdlist) < 11:
            logger.error("cmdbody error")
            return

        imei_str = cmdlist[0]
        imsi_str = cmdlist[1]
        power_str = cmdlist[2]
        selfmobile = cmdlist[3]
        phonemodel = cmdlist[4]
        firmware = cmdlist[5]
        networktype = cmdlist[6]
        carrier = cmdlist[7]
        vib = cmdlist[8]
        mute = cmdlist[9]
        location_timeoute = cmdlist[10]

        imei = int(imei_str)
        searchkey = self.KEY_IMEI_ID % (imei_str, '*')
        resultlist = self._redis.keys(searchkey)
        imeikey = None
        if len(resultlist) == 0:            
            imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
            if imeiinfo == None:
                logger.error("can't found imei in cache!!!")
                senddict = dict()
                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
                senddict['sendbuf'] = "register2:-1,%s,%s#"%(self.ERRORCODE_IMEI_NOT_EXIST,datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                self.send_to_publish_queue(senddict)

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

        insertdict = dict()
        insertdict['imsi'] = imsi_str
        insertdict['battery_level'] = int(power_str)
        if selfmobile != '':
            insertdict['mobile'] = selfmobile
        insertdict['phonemodel'] = phonemodel
        insertdict['firmware'] = firmware
        insertdict['networktype'] = networktype
        insertdict['carrier'] = carrier
        insertdict['alert_motor'] = int(vib)
        insertdict['alert_ring'] = int(mute)
        insertdict['location_interval'] = int(location_timeoute)
        insertdict['protocol'] = self.PROTOCOL_VERSION_2

        self.collect_gearinfo.update_one({'imei':imei},{'$set':insertdict})
        self._redis.hmset(imeikey,insertdict)

        
        self.set_online_flag(imei_str)
        #回送消息
        senddict = dict()
        senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
        senddict['sendbuf'] = "register2:0,0,%s#" % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        self.send_to_publish_queue(senddict)


    def _proc_gps(self, cmdbody):
        logger.debug("into _proc_gps : %s" % (cmdbody))
        if cmdbody is '' or cmdbody == '':
            logger.error("cmdbody error")
            return

        cmdlist = cmdbody.split(',')
        if len(cmdlist) != 8:
            logger.error("cmdbody error")
            return

        imei_str = cmdlist[0]
        imei = int(imei_str)
        searchkey = self.KEY_IMEI_ID % (imei_str, '*')
        resultlist = self._redis.keys(searchkey)
        if len(resultlist) == 0:
            imei = int(imei_str)
            imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
            if imeiinfo == None:
                logger.error("can't found imei in cache!!!")
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

        self.set_online_flag(imei_str)
        timet               = cmdlist[1]
        time_device         = int(cmdlist[1])
        battery_level       = cmdlist[2]
        latitude            = float(cmdlist[3])
        longitude           = float(cmdlist[4])
        altitude            = float(cmdlist[5])
        angle               = float(cmdlist[6])
        vt                  = float(cmdlist[7])
        location_type       = self.LOCATION_TYPE_GPS
        timestamp           = datetime.datetime.now()


        insertdict = dict()
        insertdict['imei'] = imei
        insertdict['timet'] = timet
        insertdict['time_device'] = time_device
        insertdict['battery_level'] = battery_level
        insertdict['longitude'] = longitude
        insertdict['latitude'] = latitude
        insertdict['altitude'] = altitude
        insertdict['vt'] = vt
        insertdict['location_type'] = location_type
        insertdict['timestamp'] = timestamp
        insertdict['angle'] = angle

        insertobj = self.collect_gpsinfo.insert_one(insertdict)

        #更新到gearinfo中去
        if insertobj:
            updatedict = dict()
            updatedict['battery_level'] = battery_level
            updatedict['last_long'] = longitude
            updatedict['last_lat'] = latitude
            updatedict['last_at'] = altitude
            updatedict['last_vt'] = vt
            updatedict['last_angle'] = angle
            updatedict['last_location_objectid'] = insertobj.inserted_id.__str__()
            updatedict['last_location_type'] = self.LOCATION_TYPE_GPS
            updatedict['last_location_time'] = timestamp.__str__()

            self._redis.hmset(imeikey, updatedict)
            self.collect_gearinfo.update_one({'imei':imei},{'$set':updatedict})
        #查看是否需要围栏服务
        if 'alarm_enable' in imeiinfo and imeiinfo['alarm_enable'] == '1':
            body = dict()
            body['imei'] = imei
            body['latitude'] = latitude
            body['longitude'] = longitude
            body['imeikey'] = imeikey
            action = dict()
            action['body'] = body
            action['version'] = '1.0'
            action['action_cmd'] = 'check_location'
            action['seq_id'] = '%d' % random.randint(0,10000)
            action['from'] = ''
            if 'session' in self._config:
                self._sendMessage(self._config['session']['Consumer_Queue_Name'], json.dumps(action))
        #检查电量是否需要报警 发送报警信息给app
        if int(battery_level)<self.BATTERY_LEVEL_ALERT:
            if 'notify' in self._config:
                body = dict()
                body['imei'] = imei
                body['imeikey'] = imeikey
                body['notify_id'] = self.NOTIFY_CODE_LOW_BATTERY
                action = dict()
                action['body'] = body
                action['version'] = '1.0'
                action['action_cmd'] = 'send_notify'
                action['seq_id'] = '%d' % random.randint(0,10000)
                action['from'] = ''          
                self._sendMessage(self._config['notify']['Consumer_Queue_Name'], json.dumps(action))            

        #检查是否有紧急定位标志如果有就删除，然后通知用户定位成功
        locationkey = self.KEY_IMEI_CMD % (imei_str, self.CMD_TYPE_START_LOCATION)
        if self._redis.exists(locationkey):
            cmdinfo = self._redis.hgetall(locationkey)
            if 'notify' in self._config:
                body = dict()
                body['imei'] = imei
                body['imeikey'] = imeikey
                if 'tid' in cmdinfo:
                    body['tid'] = cmdinfo.get('tid')
                body['notify_id'] = self.NOTIFY_CODE_LOCATION_OK
                action = dict()
                action['body'] = body
                action['version'] = '1.0'
                action['action_cmd'] = 'send_notify'
                action['seq_id'] = '%d' % random.randint(0,10000)
                action['from'] = ''          
                self._sendMessage(self._config['notify']['Consumer_Queue_Name'], json.dumps(action))            
            self._redis.delete(locationkey)
            

    def _proc_lbs(self, cmdbody):
        logger.debug("into _proc_lbs : %s" % (cmdbody))
        if cmdbody is '' or cmdbody == '':
            logger.error("cmdbody error")
            return

        cmdlist = cmdbody.split(',')
        #判断imei合法性
        imei_str = cmdlist[0]
        imei = int(imei_str)
        searchkey = self.KEY_IMEI_ID % (imei_str, '*')
        resultlist = self._redis.keys(searchkey)
        if len(resultlist) == 0:
            imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
            if imeiinfo == None:
                logger.error("can't found imei in cache!!!")
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

        self.set_online_flag(imei_str)

        #解码
        timet               = cmdlist[1]
        time_device         = int(cmdlist[1])
        battery_level       = cmdlist[2]
        accessMCC           = cmdlist[3]
        accessMNC           = cmdlist[4]
        accessLAC           = cmdlist[5]
        accessCellid        = cmdlist[6]
        accessSignal        = cmdlist[7]
        nearbylist          = cmdlist[8]
        current_idx         = 8
        accesslbsinfo = dict()
        accesslbsinfo['mnc'] = accessMNC
        accesslbsinfo['mcc'] = accessMCC
        accesslbsinfo['lac'] = accessLAC
        accesslbsinfo['cellid'] = accessCellid
        accesslbsinfo['gsm_signal'] = accessSignal
        lbslist = list()
        lbslist.append(accesslbsinfo)
        for i in xrange(0,int(nearbylist)):
            current_idx+= 1
#            print current_idx
#            print cmdlist[current_idx]
            tmplist = cmdlist[current_idx].split('|')
            lbsinfo = dict()
            lbsinfo['mcc'] = tmplist[0]
            lbsinfo['mnc'] = tmplist[1]
            lbsinfo['lac'] = tmplist[2]
            lbsinfo['cellid'] = tmplist[3]
            lbsinfo['gsm_signal'] = tmplist[4]
            lbslist.append(lbsinfo)
        current_idx = current_idx+1
        nearbywifi = cmdlist[current_idx]
        wifilist = list()
        for i in xrange(0,int(nearbywifi)):
            current_idx+= 1
            tmplist = cmdlist[current_idx].split('|')
            wifiinfo = dict()
            wifiinfo['wifimac'] = tmplist[0]
            wifiinfo['wifisignal'] = tmplist[1]
            wifilist.append(wifiinfo)

        location_type       = self.LOCATION_TYPE_LBS
        timestamp           = datetime.datetime.now()
        #存数据库
        insertdict = dict()
        insertdict['lbs'] = lbslist
        insertdict['timet'] = timet
        insertdict['time_device'] = time_device
        insertdict['location_type'] = location_type
        insertdict['timestamp'] = timestamp
        insertdict['battery_level'] = battery_level
        insertdict['wifi'] = wifilist
        insertdict['imei'] = imei
        insertobj = self.collect_lbsinfo.insert_one(insertdict)

        #更新到gearinfo中去
        if insertobj:
            updatedict = dict()
            updatedict['battery_level'] = battery_level
            updatedict['last_lbs_objectid'] = insertobj.inserted_id.__str__()
            self._redis.hmset(imeikey, updatedict)
            self.collect_gearinfo.update_one({'imei':imei},{'$set':updatedict})
        #发送基站wifi定位解析的请求：
            body = dict()
            body['imei'] = imei
            body['imeikey'] = imeikey
            body['last_lbs_objectid'] = imeiinfo.get('last_lbs_objectid')
            body['lbs_objectid'] = insertobj.inserted_id.__str__()
            body['lbs'] = lbslist
            body['wifi'] = wifilist

            action = dict()
            action['body'] = body
            action['version'] = '1.0'
            action['action_cmd'] = 'lbs_location'
            action['seq_id'] = '%d' % random.randint(0,10000)
            action['from'] = ''
            if accessMCC == "460":
                if 'lbs_proxy' in self._config:
                    self._sendMessage(self._config['lbs_proxy']['Consumer_Queue_Name'], json.dumps(action))
            # else:
                # self._sendMessage('W:Queue:LbsGoogleProxy', json.dumps(action))
                
        #检查电量是否需要报警 发送报警信息给app
        if int(battery_level)<self.BATTERY_LEVEL_ALERT:
            if 'notify' in self._config:
                body = dict()
                body['imei'] = imei
                body['imeikey'] = imeikey
                body['notify_id'] = self.NOTIFY_CODE_LOW_BATTERY
                action = dict()
                action['body'] = body
                action['version'] = '1.0'
                action['action_cmd'] = 'send_notify'
                action['seq_id'] = '%d' % random.randint(0,10000)
                action['from'] = ''          
                self._sendMessage(self._config['notify']['Consumer_Queue_Name'], json.dumps(action))            
        #检查是否有紧急定位标志如果有就删除，然后通知用户定位成功
        locationkey = self.KEY_IMEI_CMD % (imei_str, self.CMD_TYPE_START_LOCATION)
        if self._redis.exists(locationkey):
            cmdinfo = self._redis.hgetall(locationkey)
            if 'notify' in self._config:
                body = dict()
                body['imei'] = imei
                body['imeikey'] = imeikey
                if 'tid' in cmdinfo:
                    body['tid'] = cmdinfo.get('tid')
                body['notify_id'] = self.NOTIFY_CODE_LOCATION_OK
                action = dict()
                action['body'] = body
                action['version'] = '1.0'
                action['action_cmd'] = 'send_notify'
                action['seq_id'] = '%d' % random.randint(0,10000)
                action['from'] = ''          
                self._sendMessage(self._config['notify']['Consumer_Queue_Name'], json.dumps(action))            

            self._redis.delete(locationkey)


    def _proc_setnbr(self, cmdbody):
        logger.debug("into _proc_setnbr : %s" % (cmdbody))
        if cmdbody is '' or cmdbody == '':
            logger.error("cmdbody error")
            return

        cmdlist = cmdbody.split(',')
        imei_str = cmdlist[0]
        imei = int(imei_str)
        searchkey = self.KEY_IMEI_ID % (imei_str, '*')
        resultlist = self._redis.keys(searchkey)
        if len(resultlist) == 0:
            imei = int(imei_str)
            imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
            if imeiinfo == None:
                logger.error("can't found imei in cache!!!")
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

        self.set_online_flag(imei_str)

        cmdkey = self.KEY_IMEI_CMD % (imei_str, self.CMD_TYPE_SET_NUMBER)
        if self._redis.exists(cmdkey):
            cmdinfo = self._redis.hgetall(cmdkey)
            setdict = dict()
            if 'soslist' in cmdinfo:
                setdict['sos_number'] = cmdinfo.get('soslist')
            if 'monitorlist' in cmdinfo:
                setdict['monitor_number'] = cmdinfo.get('monitorlist')
            if 'friendlist' in cmdinfo:
                setdict['friend_number'] = cmdinfo.get('friendlist')
            
            self._redis.hmset(imeikey, setdict)
            self.collect_gearinfo.update_one({'imei':imei},{'$set':setdict})
            self._redis.delete(cmdkey)
            #通知到用户
            if 'notify' in self._config:
                body = dict()
                body['imei'] = imei
                body['imeikey'] = imeikey
                if 'tid' in cmdinfo:
                    body['tid'] = cmdinfo['tid']
                body['notify_id'] = self.NOTIFY_CODE_SET_SOS_OK
                action = dict()
                action['body'] = body
                action['version'] = '1.0'
                action['action_cmd'] = 'send_notify'
                action['seq_id'] = '%d' % random.randint(0,10000)
                action['from'] = ''          
                self._sendMessage(self._config['notify']['Consumer_Queue_Name'], json.dumps(action))


    def _proc_alert(self, cmdbody):
        logger.debug("into _proc_alert : %s" % (cmdbody))
        if cmdbody is '' or cmdbody == '':
            logger.error("cmdbody error")
            return

        cmdlist = cmdbody.split(',')
        imei_str = cmdlist[0]
        imei = int(imei_str)
        searchkey = self.KEY_IMEI_ID % (imei_str, '*')
        resultlist = self._redis.keys(searchkey)
        if len(resultlist) == 0:
            imei = int(imei_str)
            imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
            if imeiinfo == None:
                logger.error("can't found imei in cache!!!")
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
        self.set_online_flag(imei_str)



        cmdkey = self.KEY_IMEI_CMD % (imei_str, self.CMD_TYPE_SET_CLOCK)
        if self._redis.exists(cmdkey):
            cmdinfo = self._redis.hgetall(cmdkey)
            insertdict = dict()
            insertdict['imei'] = imei
            insertdict['firedate'] = cmdinfo.get('firedate')
            insertdict['content'] = cmdinfo.get('content')
            insertdict['vid'] = imeiinfo.get('vid')
            insertdict['memberid'] = cmdinfo.get('memberid')
            self.collect_gearclock.insert_one(insertdict)
            self._redis.delete(cmdkey)
            #通知到用户
            if 'notify' in self._config:
                body = dict()
                body['imei'] = imei
                body['imeikey'] = imeikey
                if 'tid' in cmdinfo:
                    body['tid'] = cmdinfo['tid']
                body['notify_id'] = self.NOTIFY_CODE_SET_CLOCK_OK
                action = dict()
                action['body'] = body
                action['version'] = '1.0'
                action['action_cmd'] = 'send_notify'
                action['seq_id'] = '%d' % random.randint(0,10000)
                action['from'] = ''
                self._sendMessage(self._config['notify']['Consumer_Queue_Name'], json.dumps(action))


    def _proc_getnumber(self, cmdbody):
        logger.debug("into _proc_getnumber : %s" % (cmdbody))
        if cmdbody is '' or cmdbody == '':
            logger.error("cmdbody error")
            return

        cmdlist = cmdbody.split(',')

        imei_str = cmdlist[0]
        imei = int(imei_str)

        searchkey = self.KEY_IMEI_ID % (imei_str, '*')
        resultlist = self._redis.keys(searchkey)
        imeikey = None
        imeiinfo = None
        if len(resultlist) == 0:            
            imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
            if imeiinfo == None:
                logger.error("can't found imei in cache!!!")
                senddict = dict()
                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
                senddict['sendbuf'] = "GETNUMBER:-1#"
                self.send_to_publish_queue(senddict)

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

        
        self.set_online_flag(imei_str)
        soscount = 0
        monitorcount = 0
        friendcount = 0
        soslist = list()
        monitorlist = list()
        friendlist = list()
        if 'sos_number' in imeiinfo:
            sosnumberstr = imeiinfo['sos_number']
            sosnumberlist = sosnumberstr.split(',')
            soscount = len(sosnumberlist)
            if soscount == 3:
                soslist.append("%s|" %(sosnumberlist[0]))
                soslist.append("%s|" %(sosnumberlist[1]))
                soslist.append("%s|" %(sosnumberlist[2]))
            elif soscount == 6:
                soslist.append("%s|%s" %(sosnumberlist[0],urllib.unquote(sosnumberlist[3].encode('utf-8')).decode('utf-8')))
                soslist.append("%s|%s" %(sosnumberlist[1],urllib.unquote(sosnumberlist[4].encode('utf-8')).decode('utf-8')))
                soslist.append("%s|%s" %(sosnumberlist[2],urllib.unquote(sosnumberlist[5].encode('utf-8')).decode('utf-8')))
            else:
                soscount = 0
        if 'monitor_number' in imeiinfo:
            monitorstr = imeiinfo['monitor_number']
            monitornumberlist = monitorstr.split(',')
            monitorcount = len(monitornumberlist)
            if monitorcount == 2:
                monitorlist.append("%s|" %(monitornumberlist[0]))
                monitorlist.append("%s|" %(monitornumberlist[1]))
            elif monitorcount == 4:
                monitorlist.append("%s|%s" %(monitornumberlist[0],urllib.unquote(monitornumberlist[2].encode('utf-8')).decode('utf-8')))
                monitorlist.append("%s|%s" %(monitornumberlist[1],urllib.unquote(monitornumberlist[3].encode('utf-8')).decode('utf-8')))
            else:
                monitorcount = 0

        if 'friend_number' in imeiinfo:
            friendnumberstr = imeiinfo['friend_number']
            friendnumberlist = friendnumberstr.split(',')
            friendcount = len(friendnumberlist)
            if friendcount == 5:
                friendlist.append("%s|" %(friendnumberlist[0]))
                friendlist.append("%s|" %(friendnumberlist[1]))
                friendlist.append("%s|" %(friendnumberlist[2]))
                friendlist.append("%s|" %(friendnumberlist[3]))
                friendlist.append("%s|" %(friendnumberlist[4]))
            elif friendcount == 10:
                friendlist.append("%s|%s" %(friendnumberlist[0],urllib.unquote(friendnumberlist[5].encode('utf-8')).decode('utf-8')))
                friendlist.append("%s|%s" %(friendnumberlist[1],urllib.unquote(friendnumberlist[6].encode('utf-8')).decode('utf-8')))
                friendlist.append("%s|%s" %(friendnumberlist[2],urllib.unquote(friendnumberlist[7].encode('utf-8')).decode('utf-8')))
                friendlist.append("%s|%s" %(friendnumberlist[3],urllib.unquote(friendnumberlist[8].encode('utf-8')).decode('utf-8')))
                friendlist.append("%s|%s" %(friendnumberlist[4],urllib.unquote(friendnumberlist[9].encode('utf-8')).decode('utf-8')))
            else:
                friendcount = 0
        
        sendbuf = "GETNUMBER:%d,%s,%d,%s,%d,%s#" % (len(soslist),','.join(soslist),len(monitorlist),','.join(monitorlist), len(friendlist), ','.join(friendlist))
        logger.debug(sendbuf)
        #回送消息
        senddict = dict()
        senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
        senddict['sendbuf'] = sendbuf.encode('utf-8')
        senddict['encode'] = 'utf-8'
        senddict['protocol'] = self.PROTOCOL_VERSION_2
        self.send_to_publish_queue(senddict)

    def _proc_calllog(self, cmdbody):
        logger.debug("into _proc_calllog : %s" % (cmdbody))
        if cmdbody is '' or cmdbody == '':
            logger.error("cmdbody error")
            return

        cmdlist = cmdbody.split(',')

        imei_str = cmdlist[0]
        imei = int(imei_str)

        searchkey = self.KEY_IMEI_ID % (imei_str, '*')
        resultlist = self._redis.keys(searchkey)
        imeikey = None
        imeiinfo = None
        if len(resultlist) == 0:            
            imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
            if imeiinfo == None:
                logger.error("can't found imei in cache!!!")
                senddict = dict()
                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
                senddict['sendbuf'] = "GETNUMBER:-1#"
                self.send_to_publish_queue(senddict)

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

        
        self.set_online_flag(imei_str)
        direction = cmdlist[1]
        calltype = cmdlist[2]
        cg = cmdlist[3]
        cd = cmdlist[4]
        state = cmdlist[5]
        duration = cmdlist[6]
        timet = cmdlist[7]

        logger.debug("into _proc_generalcmd : %s" % (cmdbody))
        celled = cmdlist[2]
        answer = int(cmdlist[3][:-1])
        insertcallrecord = dict()
        insertcallrecord['imei'] = imei
        insertcallrecord['vid'] = imeiinfo['vid']
        insertcallrecord['number'] = cd
        insertcallrecord['caller'] = cg
        insertcallrecord['calltype'] = calltype
        insertcallrecord['direction'] = int(direction)
        insertcallrecord['answer'] = int(state)
        insertcallrecord['timestamp'] = datetime.datetime.now()
        insertcallrecord['duration'] = int(duration)
        insertcallrecord['time_device'] = int(timet)

        self.collect_callrecord.insert_one(insertcallrecord)
        if answer == self.CALL_UNCONNECT:
            if 'notify' in self._config:
                #启动通知服务
                body = dict()
                body['imei'] = imei
                body['imeikey'] = imeikey
                body['notify_id'] = self.NOTIFY_CODE_SOS_UNCONNECT
                action = dict()
                action['body'] = body
                action['version'] = '1.0'
                action['action_cmd'] = 'send_notify'
                action['seq_id'] = '%d' % random.randint(0,10000)
                action['from'] = ''
                self._sendMessage(self._config['notify']['Consumer_Queue_Name'], json.dumps(action))



    def _proc_keepalive(self, cmdbody):
        logger.debug("into _proc_keepalive : %s" % (cmdbody))
        if cmdbody is '' or cmdbody == '':
            logger.error("cmdbody error")
            return

        cmdlist = cmdbody.split(',')

        imei_str = cmdlist[0]
        imei = int(imei_str)
        interval = int(cmdlist[1])

        searchkey = self.KEY_IMEI_ID % (imei_str, '*')
        resultlist = self._redis.keys(searchkey)
        imeikey = None
        if len(resultlist) == 0:            
            imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
            if imeiinfo == None:
                logger.error("can't found imei in cache!!!")
                senddict = dict()
                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
                senddict['sendbuf'] = "register2:-1,%s#"%(self.ERRORCODE_IMEI_NOT_EXIST)
                self.send_to_publish_queue(senddict)

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

        if interval == 0:
            interval = self.GEAR_ONLINE_TIMEOUT
        self.set_online_flag2(imei_str,interval)
        #回送消息
        senddict = dict()
        senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
        senddict['sendbuf'] = "KEEPALIVE#"
        self.send_to_publish_queue(senddict)


    def _proc_inforeq(self, cmdbody):
        logger.debug("into _proc_inforeq : %s" % (cmdbody))
        if cmdbody is '' or cmdbody == '':
            logger.error("cmdbody error")
            return

        cmdlist = cmdbody.split(',')

        imei_str = cmdlist[0]
        imei = int(imei_str)

        language = cmdlist[1]

        searchkey = self.KEY_IMEI_ID % (imei_str, '*')
        resultlist = self._redis.keys(searchkey)
        imeikey = None
        imeiinfo = None
        if len(resultlist) == 0:            
            imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
            if imeiinfo == None:
                logger.error("can't found imei in cache!!!")
                senddict = dict()
                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
                senddict['sendbuf'] = "INFOREQ:#"
                self.send_to_publish_queue(senddict)

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

        
        self.set_online_flag(imei_str)

        #查找所有的通知
        infolist = self.collect_infotemp.find({'state':1})
        sendbuf = ""
        for infotemp in infolist:
            logger.debug(infotemp)
            content = ""
            if language in infotemp['info_desc']:
                content = infotemp['info_desc'][language]
            else:
                content = infotemp['info_desc']['chs']
            if sendbuf == "":
                sendbuf = "%s|%s" % (content, infotemp['info_id'])
            else:
                sendbuf = "%s,%s|%s" % (sendbuf, content, infotemp['info_id']) 

        sendbuf = "INFOREQ:%s#" % (sendbuf)
        logger.debug(sendbuf)
        #回送消息
        senddict = dict()
        senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
        senddict['sendbuf'] = sendbuf.encode('utf-8')
        senddict['encode'] = 'utf-8'
        senddict['protocol'] = self.PROTOCOL_VERSION_2
        self.send_to_publish_queue(senddict)

    def _proc_infodec(self, cmdbody):
        logger.debug("into _proc_infodec : %s" % (cmdbody))
        if cmdbody is '' or cmdbody == '':
            logger.error("cmdbody error")
            return

        cmdlist = cmdbody.split(',')

        imei_str = cmdlist[0]
        imei = int(imei_str)
        uploadid = cmdlist[1]

        searchkey = self.KEY_IMEI_ID % (imei_str, '*')
        resultlist = self._redis.keys(searchkey)
        imeikey = None
        imeiinfo = None
        if len(resultlist) == 0:            
            imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
            if imeiinfo == None:
                logger.error("can't found imei in cache!!!")
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

        
        self.set_online_flag(imei_str)
        insertdict = dict()
        insertdict['imei'] = imei
        insertdict['imei_str'] = imei_str
        insertdict['info_id'] = uploadid
        insertdict['timestamp'] = datetime.datetime.now()

        stateinfo = self.collect_infostate.find_one({'imei':imei})
        if stateinfo is None:
            self.collect_infostate.insert_one(insertdict)
        else:
            self.collect_infostate.update_one({'imei':imei},{'$set':{'info_id':uploadid,'timestamp':datetime.datetime.now()}})
        self.collect_infolog.insert_one(insertdict)


if __name__ == "__main__":

    fileobj = open("/opt/Keeprapid/Wishoney/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()

    thread_count = 1
    if _config is not None and 'devicelogic' in _config and _config['devicelogic'] is not None:
        if 'thread_count' in _config['devicelogic'] and _config['devicelogic']['thread_count'] is not None:
            thread_count = int(_config['devicelogic']['thread_count'])

    for i in xrange(0, thread_count):
        logic = DeviceLogic(i)
        logic.setDaemon(True)
        logic.start()

    while 1:
        time.sleep(1)


