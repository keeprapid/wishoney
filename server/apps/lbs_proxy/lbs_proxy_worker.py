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
import urllib2
import logging
import logging.config
import uuid
import redis
import hashlib
import urllib
import base64
import random
import math
from bson.objectid import ObjectId
from math import sqrt,sin,cos,atan2

x_pi = 3.14159265358979324 * 3000.0 / 180.0
pi = 3.1415926535897932384626  # π
a = 6378245.0  # 长半轴
ee = 0.00669342162296594323  # 扁率
if '/opt/Keeprapid/Wishoney/server/apps/common' not in sys.path:
    sys.path.append('/opt/Keeprapid/Wishoney/server/apps/common')
import workers

logging.config.fileConfig("/opt/Keeprapid/Wishoney/server/conf/log.conf")
logger = logging.getLogger('wishoney')


class LbsProxy(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
#        super(LbsProxy, self).__init__()
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("LbsProxy :running in __init__")

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
        self.recv_queue_name = "W:Queue:LbsProxy"
        if 'lbs_proxy' in _config:
            if 'Consumer_Queue_Name' in _config['lbs_proxy']:
                self.recv_queue_name = _config['lbs_proxy']['Consumer_Queue_Name']
        self.publish_queue_name = "W:Queue:MQTTPub"
        if 'mqtt_publish' in _config:
            if 'Consumer_Queue_Name' in _config['mqtt_publish']:
                self.publish_queue_name = _config['mqtt_publish']['Consumer_Queue_Name']

        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
#        self.locationconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.locationconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.locationdb = self.locationconn.location
        self.collect_gpsinfo = self.locationdb.gpsinfo
        self.collect_lbsinfo = self.locationdb.lbsinfo
        self.collect_lbslog = self.locationdb.lbslog

#        self.gearconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.gearconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.geardb = self.gearconn.gearcenter
        self.collect_gearinfo = self.geardb.gearinfo


    def run(self):
        logger.debug("Start LbsProxy pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
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

        if action_cmd == 'lbs_location':
            self._proc_action_lbs_location2(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = '40000'

        return

    def _proc_action_lbs_location2(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'register', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                body['imei'] = imei
                body['imeikey'] = imeikey
                body['lbs_objectid'] = insertobj.__str__()
                body['last_lbs_objectid'] = imeiinfo.get('last_lbs_objectid')
                body['lbs'] = lbsinfolist
                body['wifi'] = wifiinfolist

            insertlbsinfo['mnc'] = mnc
            insertlbsinfo['mcc'] = mcc
            insertlbsinfo['lac'] = lbs['lac']
            insertlbsinfo['cellid'] = lbs['cellid']
            insertlbsinfo['gsm_signal'] = lbs['gsm_signal']
            wifiinfo1['wifimac'] = wifimac1
            wifiinfo1['wifisignal'] = wifisignal1
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                }
        '''
        logger.debug(" into _proc_action_lbs_location2 action_body:[%s]:%s"%(version,action_body))
        try:
#        if 1:
            
            if ('imeikey' not in action_body) or  ('imei' not in action_body) or  ('lbs_objectid' not in action_body) or  ('lbs' not in action_body) or  ('wifi' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['imeikey'] is None or action_body['imeikey'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['imei'] is None or action_body['imei'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['lbs_objectid'] is None or action_body['lbs_objectid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['lbs'] is None or action_body['lbs'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['wifi'] is None or action_body['wifi'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return

            last_lbs_objectid = None
            if 'last_lbs_objectid' in action_body and action_body['last_lbs_objectid'] is not None:
                last_lbs_objectid = action_body['last_lbs_objectid']

            imeikey = action_body['imeikey']
            lbs_objectid = action_body['lbs_objectid']
            current_lbs = action_body['lbs']
            current_wifi = action_body['wifi']
            imei_str = action_body['imei']
            imei = int(imei_str)

            tni = self._redis.hgetall(imeikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return
            current_lbs_info = self.collect_lbsinfo.find_one({'_id':ObjectId(lbs_objectid)})
                
            #去基站定位
            param = dict()
            accesscell = current_lbs.pop(0)
            if accesscell is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return

            signal = int(accesscell.get("gsm_signal").encode('ascii'))
            if signal>0:
                accesscellparam = "bts=%s,%s,%s,%s,-%s" % (accesscell.get("mcc").encode('ascii'), accesscell.get("mnc").encode('ascii'),accesscell.get("lac").encode('ascii'),accesscell.get("cellid").encode('ascii'),accesscell.get("gsm_signal").encode('ascii'))
            else:
                accesscellparam = "bts=%s,%s,%s,%s,%s" % (accesscell.get("mcc").encode('ascii'), accesscell.get("mnc").encode('ascii'),accesscell.get("lac").encode('ascii'),accesscell.get("cellid").encode('ascii'),accesscell.get("gsm_signal").encode('ascii'))
            celltowers = list()
            nearbtsparamlist = list()
            for lbs in current_lbs:
                signal = int(lbs.get("gsm_signal").encode('ascii'))
                if signal>0:
                    nearbtsparamlist.append("%s,%s,%s,%s,-%s" % (lbs.get("mcc").encode('ascii'), lbs.get("mnc").encode('ascii'),lbs.get("lac").encode('ascii'),lbs.get("cellid").encode('ascii'),lbs.get("gsm_signal").encode('ascii')))
                else:
                    nearbtsparamlist.append("%s,%s,%s,%s,%s" % (lbs.get("mcc").encode('ascii'), lbs.get("mnc").encode('ascii'),lbs.get("lac").encode('ascii'),lbs.get("cellid").encode('ascii'),lbs.get("gsm_signal").encode('ascii')))
                    
            nearbtsparam = None
            if len(nearbtsparamlist)>0:
                nearbtsparam = "nearbts=%s"%('|'.join(nearbtsparamlist))

            wifilist = list()
            for wifi in current_wifi:
                wifilist.append("%s,%s," % ( wifi.get("wifimac").encode('ascii'), wifi.get("wifisignal").encode('ascii')))
            macsparam = None
            if len(wifilist)>0:
                macsparam = "macs=%s" % ('|'.join(wifilist))

            url = "%skey=%s&accesstype=0&imei=%s&cdma=0&network=GSM&output=json&%s" % (self.LBS_SERVICE_AMAP_URL,self.LBS_SERVICE_AMAP_KEY,imei_str,accesscellparam)
            if nearbtsparam is not None:
                url = "%s&%s" % (url, nearbtsparam)

            if macsparam is not None:
                url = "%s&%s" % (url, macsparam)

            logger.debug("url = %s" %(url))
#            request = urllib2.Request(url)
#            response = urllib2.urlopen(request)
#            data = response.read()
            resp = urllib2.urlopen(url,timeout=5)
            logger.debug(resp)
            data = resp.read()
            logger.debug("response = %s" %(data))
            datajson = json.loads(data)
#            data = '{"location":{"address":{"region":"\xe7\xa6\x8f\xe5\xbb\xba\xe7\x9c\x81","county":"\xe6\x80\x9d\xe6\x98\x8e\xe5\x8c\xba","street":"\xe9\x87\x91\xe5\xb1\xb1\xe8\xa1\x97\xe9\x81\x93","street_number":"\xe7\x9f\xb3\xe6\x9d\x91","city":"\xe5\x8e\xa6\xe9\x97\xa8\xe5\xb8\x82","country":"\xe4\xb8\xad\xe5\x9b\xbd"},"addressDescription":"\xe7\xa6\x8f\xe5\xbb\xba\xe7\x9c\x81\xe5\x8e\xa6\xe9\x97\xa8\xe5\xb8\x82\xe6\x80\x9d\xe6\x98\x8e\xe5\x8c\xba\xe6\x98\x8e\xe5\x8f\x91\xe5\x9f\x8e\xe9\x87\x91\xe5\xb1\xb1\xe8\xa1\x97\xe9\x81\x93\xe5\xb2\xad\xe5\x85\x9c\xe6\x96\xb0\xe6\x9d\x91\xe6\x98\x8e\xe5\x8f\x91\xe5\x9f\x8e\xe5\x8c\x97\xe9\x97\xa8\xe8\xa5\xbf\xe5\x8c\x97","longitude":118.1741300,"latitude":24.4838400,"accuracy":"3000"},"access_token":null,"ErrCode":"0"}'
#            datajson = json.loads(data)
            if datajson.get('status') and datajson.get('info'):
                if datajson.get('status') == '1' and datajson.get('info') == 'OK':
                    result = datajson.get('result')
                    if result is not None:
                        if result.get('type') == '0':
                            retdict['error_code'] = self.ERRORCODE_LBS_FAIL
                            return
                        location = result.get('location')
                        lat = '0'
                        log = '0'
                        acc = '0'
                        address = dict()
                        if location:
                            tmpllist = location.split(',')
                            if len(tmpllist) == 2:
                                log = tmpllist[0]
                                lat = tmpllist[1]

                        if result.get('radius') is not None:
                            acc = result.get('radius')
                        if result.get('desc') is not None:
                            address = result.get('desc')

                        #由高德地图坐标转到百度地图坐标
                        wgs = self.gcj02towgs84(float(log), float(lat))
                        logger.debug('convert gcj02 2 wgs = %s' % wgs.__str__())
#                        transurl = "http://api.map.baidu.com/geoconv/v1/?coords=%s,%s&from=3&to=5&ak=ZnjBnGQS0Ty18x8xOXDs9GOk" % (log, lat)
#                        logger.debug("transurl = %s" %(transurl))
                        #tr = urllib2.Request(transurl)
#                        tdata = urllib2.urlopen(transurl,timeout=5).read()
                        #tdata = tres.read()
#                        logger.debug("tres = %s" %(tdata))
#                        transresult = json.loads(tdata)

#                        if transresult.get('status') is None or transresult.get('status') != 0:
#                            retdict['error_code'] = self.ERRORCODE_LBS_FAIL
#                            return
#
#                        baidulat = transresult['result'][0]['y']
#                        baidulong = transresult['result'][0]['x']
#                        wgs = self.convertbaidu2wgs( baidulong, baidulat)
#                        logger.debug('convert baidu 2 wgs = %s' % wgs.__str__())
                        wgslong = wgs[0]
                        wgslat = wgs[1]

                        self.collect_lbsinfo.update_one({'_id':ObjectId(lbs_objectid)},{'$set':{'latitude':wgslat,'longitude':wgslong,'accuracy':acc, 'address':address}})
                        insertdict = dict()
                        insertdict['lbs'] = current_lbs
                        insertdict['imei'] = imei
                        insertdict['gpsop'] = 0
                        insertdict['timet'] = current_lbs_info.get('timet')
                        insertdict['battery_level'] = current_lbs_info.get('battery_level')
                        insertdict['longitude'] = wgslong
                        insertdict['latitude'] = wgslat
                        insertdict['altitude'] = 0
                        insertdict['vt'] = 0
                        insertdict['location_type'] = self.LOCATION_TYPE_LBS
                        insertdict['timestamp'] = current_lbs_info.get('timestamp')
                        insertdict['angle'] = 0
                        insertdict['wifi'] = current_wifi
                        insertdict['accuracy'] = acc
                        insertdict['address'] = address
                        insertdict['lbs_objectid'] = lbs_objectid
                        insertobj = self.collect_gpsinfo.insert_one(insertdict)

                        updatedict = dict()
                        updatedict['last_long'] = wgslong
                        updatedict['last_lat'] = wgslat
                        updatedict['last_location_objectid'] = insertobj.inserted_id.__str__()
                        updatedict['last_location_type'] = self.LOCATION_TYPE_LBS
                        updatedict['last_location_time'] = current_lbs_info.get('timestamp').__str__()
                        self._redis.hmset(imeikey, updatedict)
                        self.collect_gearinfo.update_one({'imei':imei},{'$set':updatedict})
                        #增加log
                        insertlog = dict()
                        insertlog['imei'] = imei
                        insertlog['url'] = url
                        insertlog['service'] = self.LBS_SERVICE_URL
                        insertlog['key'] = self.LBS_SERVICE_KEY
                        insertlog['response'] = data
                        insertlog['lbs_objectid'] = lbs_objectid
                        insertlog['timestamp'] = datetime.datetime.now()
                        self.collect_lbslog.insert_one(insertlog)
                        #检查围栏
                        if 'alarm_enable' in tni and tni['alarm_enable'] == '1':
                            body = dict()
                            body['imei'] = imei
                            body['latitude'] = wgslat
                            body['longitude'] = wgslong
                            body['imeikey'] = imeikey
                            action = dict()
                            action['body'] = body
                            action['version'] = '1.0'
                            action['action_cmd'] = 'check_location'
                            action['seq_id'] = '%d' % random.randint(0,10000)
                            action['from'] = ''
                            if 'session' in self._config:
                                self._sendMessage(self._config['session']['Consumer_Queue_Name'], json.dumps(action))
                        #发送定位内容给手表
                        if 'protocol' in tni and tni['protocol'] == self.PROTOCOL_VERSION_2:
                            sendbuf = "GPSCB:%f,%f,%s,%s,%s,%s,%s#" % (wgslat,wgslong,accesscell.get("mcc").encode('ascii'), accesscell.get("mnc").encode('ascii'),accesscell.get("lac").encode('ascii'),accesscell.get("cellid").encode('ascii'),accesscell.get("gsm_signal").encode('ascii'))
                            senddict = dict()
                            senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei_str)
                            senddict['sendbuf'] = sendbuf
                            senddict['encode'] = 'ascii'
                            senddict['protocol'] = self.PROTOCOL_VERSION_2
                            self._redis.lpush(self.publish_queue_name, json.dumps(senddict))

            retdict['error_code'] = '200'
            return
        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL



    def _proc_action_lbs_location1(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'register', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                body['imei'] = imei
                body['imeikey'] = imeikey
                body['lbs_objectid'] = insertobj.__str__()
                body['last_lbs_objectid'] = imeiinfo.get('last_lbs_objectid')
                body['lbs'] = lbsinfolist
                body['wifi'] = wifiinfolist

            insertlbsinfo['mnc'] = mnc
            insertlbsinfo['mcc'] = mcc
            insertlbsinfo['lac'] = lbs['lac']
            insertlbsinfo['cellid'] = lbs['cellid']
            insertlbsinfo['gsm_signal'] = lbs['gsm_signal']
            wifiinfo1['wifimac'] = wifimac1
            wifiinfo1['wifisignal'] = wifisignal1
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                }
        '''
        logger.debug(" into _proc_action_member_regist action_body:[%s]:%s"%(version,action_body))
        try:
            
            if ('imeikey' not in action_body) or  ('imei' not in action_body) or  ('lbs_objectid' not in action_body) or  ('lbs' not in action_body) or  ('wifi' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['imeikey'] is None or action_body['imeikey'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['imei'] is None or action_body['imei'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['lbs_objectid'] is None or action_body['lbs_objectid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['lbs'] is None or action_body['lbs'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['wifi'] is None or action_body['wifi'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return

            last_lbs_objectid = None
            if 'last_lbs_objectid' in action_body and action_body['last_lbs_objectid'] is not None:
                last_lbs_objectid = action_body['last_lbs_objectid']

            imeikey = action_body['imeikey']
            lbs_objectid = action_body['lbs_objectid']
            current_lbs = action_body['lbs']
            current_wifi = action_body['wifi']
            imei_str = action_body['imei']
            imei = int(imei_str)

            tni = self._redis.hgetall(imeikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return
            current_lbs_info = self.collect_lbsinfo.find_one({'_id':ObjectId(lbs_objectid)})

            if last_lbs_objectid is not None:
                last_lbs_info = self.collect_lbsinfo.find_one({'_id':ObjectId(last_lbs_objectid)})
                last_latitude = last_lbs_info.get('latitude')
                last_longitude = last_lbs_info.get('longitude')
                last_lbslist = last_lbs_info.get('lbs')
                last_wifilist = last_lbs_info.get('wifi')
                logger.debug(" last_latitude = %s, last_longitude = %s",last_latitude,last_longitude)
                if last_latitude is not None and last_longitude is not None:
                    lastlbs_set = set()
                    lastwifi_set = set()
                    currentlbs_set = set()
                    currentwifi_set = set()
                    for lbs in last_lbslist:
                        lastlbs_set.add("%s:%s:%s:%s" % (lbs.get('mnc'),lbs.get('mcc'),lbs.get('lac'),lbs.get('cellid')))
                    for wifi in last_wifilist:
                        lastwifi_set.add(wifi.get('wifimac'))
                    for lbs in current_lbs:
                        currentlbs_set.add("%s:%s:%s:%s" % (lbs.get('mnc'),lbs.get('mcc'),lbs.get('lac'),lbs.get('cellid')))
                    for wifi in current_wifi:
                        currentwifi_set.add(wifi.get('wifimac'))

                    lbsdiff = currentlbs_set.difference(lastlbs_set)
                    wifidiff = currentwifi_set.difference(lastwifi_set)
                    #如果变化值小于阈值，则用上一条的lbs中的地理位置
                    if len(currentlbs_set)<self.LBS_MAX_DIFF_COUNT or len(lastlbs_set) < self.LBS_MAX_DIFF_COUNT:
                        logger.debug("cell number not enough")
                        pass
                    else:                    
                        if (len(lbsdiff)+len(wifidiff)) <= self.LBS_MAX_DIFF_COUNT:
                            self.collect_lbsinfo.update_one({'_id':ObjectId(lbs_objectid)},{'$set':{'latitude':last_latitude,'longitude':last_longitude}})
                            insertdict = dict()
                            insertdict['lbs'] = current_lbs
                            insertdict['imei'] = imei
                            insertdict['gpsop'] = 0
                            insertdict['timet'] = current_lbs_info.get('timet')
                            insertdict['battery_level'] = current_lbs_info.get('battery_level')
                            insertdict['longitude'] = last_longitude
                            insertdict['latitude'] = last_latitude
                            insertdict['altitude'] = 0
                            insertdict['vt'] = 0
                            insertdict['location_type'] = self.LOCATION_TYPE_LBS
                            insertdict['timestamp'] = current_lbs_info.get('timestamp')
                            insertdict['angle'] = 0
                            insertdict['wifi'] = current_wifi
                            insertdict['lbs_objectid'] = lbs_objectid
                            insertobj = self.collect_gpsinfo.insert_one(insertdict)

                            updatedict = dict()
                            updatedict['last_long'] = last_longitude
                            updatedict['last_lat'] = last_latitude
                            updatedict['last_location_objectid'] = insertobj.inserted_id.__str__()
                            updatedict['last_location_type'] = self.LOCATION_TYPE_LBS
                            updatedict['last_location_time'] = current_lbs_info.get('timestamp').__str__()
                            self._redis.hmset(imeikey, updatedict)
                            self.collect_gearinfo.update_one({'imei':imei},{'$set':updatedict})
                            retdict['error_code'] = '200'
                            return
                
            #去基站定位
            param = dict()
            celltowers = list()
            for lbs in current_lbs:
                obj = dict()
                obj["cell_id"] = lbs.get("cellid").encode('ascii')
                obj["mnc"] = lbs.get("mnc").encode('ascii')
                obj["mcc"] = lbs.get("mcc").encode('ascii')
                obj["lac"] = lbs.get("lac").encode('ascii')
                obj["singalstrength"] = '-'.encode('ascii')+lbs.get("gsm_signal").encode('ascii')
                celltowers.append(obj)
            wifilist = list()
            for wifi in current_wifi:
                obj = dict()
                obj["macaddress"] = wifi.get("wifimac").encode('ascii')
                obj["singalstrength"] = wifi.get("wifisignal").encode('ascii')
                wifilist.append(obj)

            param["celltowers"] = celltowers
            param["wifilist"] = wifilist
            param["mnctype"] = "gsm"
            url = "%s?requestdata=%s&key=%s&type=%d" % (self.LBS_SERVICE_URL,param.__str__().replace("'",'"').replace(' ',''),self.LBS_SERVICE_KEY,self.LBS_Coordinate_GPS)
            logger.debug("url = %s" %(url))
            request = urllib2.Request(url)
            response = urllib2.urlopen(request)
            data = response.read()
            logger.debug("response = %s" %(data))
            datajson = json.loads(data)
#            data = '{"location":{"address":{"region":"\xe7\xa6\x8f\xe5\xbb\xba\xe7\x9c\x81","county":"\xe6\x80\x9d\xe6\x98\x8e\xe5\x8c\xba","street":"\xe9\x87\x91\xe5\xb1\xb1\xe8\xa1\x97\xe9\x81\x93","street_number":"\xe7\x9f\xb3\xe6\x9d\x91","city":"\xe5\x8e\xa6\xe9\x97\xa8\xe5\xb8\x82","country":"\xe4\xb8\xad\xe5\x9b\xbd"},"addressDescription":"\xe7\xa6\x8f\xe5\xbb\xba\xe7\x9c\x81\xe5\x8e\xa6\xe9\x97\xa8\xe5\xb8\x82\xe6\x80\x9d\xe6\x98\x8e\xe5\x8c\xba\xe6\x98\x8e\xe5\x8f\x91\xe5\x9f\x8e\xe9\x87\x91\xe5\xb1\xb1\xe8\xa1\x97\xe9\x81\x93\xe5\xb2\xad\xe5\x85\x9c\xe6\x96\xb0\xe6\x9d\x91\xe6\x98\x8e\xe5\x8f\x91\xe5\x9f\x8e\xe5\x8c\x97\xe9\x97\xa8\xe8\xa5\xbf\xe5\x8c\x97","longitude":118.1741300,"latitude":24.4838400,"accuracy":"3000"},"access_token":null,"ErrCode":"0"}'
#            datajson = json.loads(data)
            if datajson.get('ErrCode'):
                if datajson.get('ErrCode') == '0':
                    location = datajson.get('location')
                    lat = 0
                    log = 0
                    acc = '0'
                    address = dict()
                    if location:
                        if 'latitude' in location and location['latitude'] is not None:
                            lat = location['latitude']
                        if 'longitude' in location and location['longitude'] is not None:
                            log = location['longitude']
                        if 'accuracy' in location and location['accuracy'] is not None:
                            acc = location['accuracy']
                        if 'address' in location and location['address'] is not None:
                            address = location['address']
                    self.collect_lbsinfo.update_one({'_id':ObjectId(lbs_objectid)},{'$set':{'latitude':lat,'longitude':log,'accuracy':acc, 'address':address}})
                    insertdict = dict()
                    insertdict['lbs'] = current_lbs
                    insertdict['imei'] = imei
                    insertdict['gpsop'] = 0
                    insertdict['timet'] = current_lbs_info.get('timet')
                    insertdict['battery_level'] = current_lbs_info.get('battery_level')
                    insertdict['longitude'] = log
                    insertdict['latitude'] = lat
                    insertdict['altitude'] = 0
                    insertdict['vt'] = 0
                    insertdict['location_type'] = self.LOCATION_TYPE_LBS
                    insertdict['timestamp'] = current_lbs_info.get('timestamp')
                    insertdict['angle'] = 0
                    insertdict['wifi'] = current_wifi
                    insertdict['accuracy'] = acc
                    insertdict['address'] = address
                    insertdict['lbs_objectid'] = lbs_objectid
                    insertobj = self.collect_gpsinfo.insert_one(insertdict)

                    updatedict = dict()
                    updatedict['last_long'] = log
                    updatedict['last_lat'] = lat
                    updatedict['last_location_objectid'] = insertobj.inserted_id.__str__()
                    updatedict['last_location_type'] = self.LOCATION_TYPE_LBS
                    updatedict['last_location_time'] = current_lbs_info.get('timestamp').__str__()
                    self._redis.hmset(imeikey, updatedict)
                    self.collect_gearinfo.update_one({'imei':imei},{'$set':updatedict})
                    #增加log
                    insertlog = dict()
                    insertlog['imei'] = imei
                    insertlog['url'] = url
                    insertlog['service'] = self.LBS_SERVICE_URL
                    insertlog['key'] = self.LBS_SERVICE_KEY
                    insertlog['response'] = data
                    insertlog['lbs_objectid'] = lbs_objectid
                    insertlog['timestamp'] = datetime.datetime.now()
                    self.collect_lbslog.insert_one(insertlog)
                    #检查围栏
                    if 'alarm_enable' in tni and tni['alarm_enable'] == '1':
                        body = dict()
                        body['imei'] = imei
                        body['latitude'] = lat
                        body['longitude'] = log
                        body['imeikey'] = imeikey
                        action = dict()
                        action['body'] = body
                        action['version'] = '1.0'
                        action['action_cmd'] = 'check_location'
                        action['seq_id'] = '%d' % random.randint(0,10000)
                        action['from'] = ''
                        if 'session' in self._config:
                            self._sendMessage(self._config['session']['Consumer_Queue_Name'], json.dumps(action))


            retdict['error_code'] = '200'
            return
        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def transformlat(self, lng, lat):
        ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + \
            0.1 * lng * lat + 0.2 * math.sqrt(math.fabs(lng))
        ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 *
                math.sin(2.0 * lng * pi)) * 2.0 / 3.0
        ret += (20.0 * math.sin(lat * pi) + 40.0 *
                math.sin(lat / 3.0 * pi)) * 2.0 / 3.0
        ret += (160.0 * math.sin(lat / 12.0 * pi) + 320 *
                math.sin(lat * pi / 30.0)) * 2.0 / 3.0
        return ret

    def transformlng(self, lng, lat):
        ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + \
            0.1 * lng * lat + 0.1 * math.sqrt(math.fabs(lng))
        ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 *
                math.sin(2.0 * lng * pi)) * 2.0 / 3.0
        ret += (20.0 * math.sin(lng * pi) + 40.0 *
                math.sin(lng / 3.0 * pi)) * 2.0 / 3.0
        ret += (150.0 * math.sin(lng / 12.0 * pi) + 300.0 *
                math.sin(lng / 30.0 * pi)) * 2.0 / 3.0
        return ret

    def out_of_china(self, lng, lat):
        """
        判断是否在国内，不在国内不做偏移
        :param lng:
        :param lat:
        :return:
        """
        if lng < 72.004 or lng > 137.8347:
            return True
        if lat < 0.8293 or lat > 55.8271:
            return True
        return False

    def gcj02towgs84(self, lng, lat):
        """
        GCJ02(火星坐标系)转GPS84
        :param lng:火星坐标系的经度
        :param lat:火星坐标系纬度
        :return:
        """
        if self.out_of_china(lng, lat):
            return lng, lat
        dlat = self.transformlat(lng - 105.0, lat - 35.0)
        dlng = self.transformlng(lng - 105.0, lat - 35.0)
        radlat = lat / 180.0 * pi
        magic = math.sin(radlat)
        magic = 1 - ee * magic * magic
        sqrtmagic = math.sqrt(magic)
        dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * pi)
        dlng = (dlng * 180.0) / (a / sqrtmagic * math.cos(radlat) * pi)
        mglat = lat + dlat
        mglng = lng + dlng
        return [lng * 2 - mglng, lat * 2 - mglat]

    def convertbaidu2wgs(self, x ,y):
        transurl = "http://api.map.baidu.com/geoconv/v1/?coords=%f,%f&from=1&to=5&ak=ZnjBnGQS0Ty18x8xOXDs9GOk" % (x, y)
        logger.debug("convertbaidu2wgs::transurl = %s" %(transurl))
#        tdata = urllib.urlopen(transurl).read()
        tdata = urllib2.urlopen(transurl,timeout=5).read()
        logger.debug("tres = %s" %(tdata))
        transresult = json.loads(tdata)

        if transresult.get('status') is None or transresult.get('status') != 0:
            return (x,y)

        x1 = transresult['result'][0]['x']
        y1 = transresult['result'][0]['y']

        a = 2*x-x1
        b = 2*y-y1
        return (a,b)

if __name__ == "__main__":
    fileobj = open("/opt/Keeprapid/Wishoney/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()

    thread_count = 1
    if _config is not None and 'lbs_proxy' in _config and _config['lbs_proxy'] is not None:
        if 'thread_count' in _config['lbs_proxy'] and _config['lbs_proxy']['thread_count'] is not None:
            thread_count = int(_config['lbs_proxy']['thread_count'])

    for i in xrange(0, thread_count):
        process = LbsProxy(i)
        process.setDaemon(True)
        process.start()

    while 1:
        time.sleep(1)
