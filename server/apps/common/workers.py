#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:  asset_main.py
# creator:   jacob.qian
# datetime:  2013-5-31
# Ronaldo wokers类的基类

import json

import logging
#import time
import logging.config
logging.config.fileConfig("/opt/Keeprapid/Wishoney/server/conf/log.conf")
logger = logging.getLogger('wishoney')



class WorkerBase():

    ERROR_RSP_UNKOWN_COMMAND = '{"seq_id":"123456","body":{},"error_code":"40000"}'
    FILEDIR_IMG_HEAD = '/mnt/www/html/wishoney/image/'
    #errorcode define
    ERRORCODE_OK = "200"
    ERRORCODE_UNKOWN_CMD = "40000"
    ERRORCODE_SERVER_ABNORMAL = "40001"
    ERRORCODE_CMD_HAS_INVALID_PARAM = '40002'
    ERRORCODE_DB_ERROR = '40003'
    ERRORCODE_MEMBER_USERNAME_ALREADY_EXIST = '41001'
    ERRORCODE_MEMBER_PASSWORD_INVALID = '41002'
    ERRORCODE_MEMBER_NOT_EXIST = '41003'
    ERRORCODE_MEMBER_TOKEN_OOS = '41004'
    ERRORCODE_MEMBER_USER_ALREADY_EXIST = '41005'
    ERRORCODE_MEMBER_USERNAME_INVALID = '41006'
    ERRORCODE_MEMBER_SOURCE_INVALID = '41007'
    ERRORCODE_MEMBER_ALREADY_FOLLOW_GEAR = '41008'
    ERRORCODE_MEMBER_NOT_FOLLOW_GEAR = '41009'
    ERRORCODE_MEMBER_VERIFYCODE_ALREADY_INUSE = '41010'
    ERRORCODE_MEMBER_VERIFYCODE_FAILEDBY_GATEWAY = '41011'
    ERRORCODE_MEMBER_VERIFYCODE_ERROR = '41012'
    ERRORCODE_MEMBER_NOT_OWNER = '41013'
    ERRORCODE_MEMBER_NO_EMAIL = '41014'

    ERRORCODE_IMEI_NOT_EXIST = '42001'
    ERRORCODE_IMEI_OUT_MAXCOUNT = '42002'
    ERRORCODE_IMEI_HAS_OWNER = '42003'
    ERRORCODE_IMEI_STATE_OOS = '42004'
    ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW = '42005'
    ERRORCODE_IMEI_CACHE_ERROR = '42006'
    ERRORCODE_IMEI_HAS_SAME_CMD_NOT_RESPONE = '42007'
    ERRORCODE_IMEI_HAS_NO_FENCE = '42008'
    ERRORCODE_IMEI_FENCE_DEACTIVE = '42009'
    ERRORCODE_IMEI_HAS_NO_NUMBER = '42010'

    ERRORCODE_LBS_FAIL = '43001'



    MAX_LOCATION_NUMBER = 5
    #member_state
    MEMBER_STATE_INIT_ACTIVE = 0
    MEMBER_STATE_EMAIL_COMFIRM = 1

    MEMBER_PASSWORD_VERIFY_CODE = "abcdef"
    #gear
    AUTH_STATE_NEW = 0
    AUTH_STATE_ONLINE = 1
    AUTH_STATE_DISABLE = 2
    AUTH_STATE_OOS = 3

    GEAR_ONLINE_TIMEOUT = 30*60
    CLIENT_ONLINE_TIMEOUT = 600
    UNSEND_MSG_TIMEOUT = 30

    GEAR_MAX_FOLLOW_COUNT = 6

    CMD_TYPE_SET_NUMBER = 'SetSosNumber'
    CMD_TYPE_SET_CLOCK = 'SetClock'
    CMD_TYPE_START_LOCATION = 'StartLocation'
    
    #notify type
    NOTIFY_TYPE_GETBACKPASSWORD = 'GetBackPassword'
    NOTIFY_TYPE_SENDVC = 'SendVerifyCode'
    NOTIFY_TYPE_WARNNING = "Warnning"
    #redis
    KEY_TOKEN_NAME_ID = "W:tni:%s:%s:%s"
    KEY_IMEI_ID = "W:g:%s:%s"
    KEY_IMEI_ONLINE_FLAG = "W:g:o:%s"
    KEY_CLIENT_ONLINE_FLAG = 'W:c:o:%s'
    KEY_IMEI_CMD = 'W:im:%s:%s'
    KEY_IMEI_LOCATION = 'W:il:%s'
    KEY_PHONE_VERIFYCODE = 'W:vc:%s:%s'

    #email
    SEND_EMAIL_INTERVAL = 2

    #special
    ADMIN_VID = "888888888888"

    #location
    LOCATION_TYPE_GPS = '1'
    LOCATION_TYPE_LBS = '2'

    MSG_TYPE_SMS = 'SMS'

    FENCE_DIRECTION_IN = 'in'
    FENCE_DIRECTION_OUT= 'out'
    CALL_DIRECTION_WATCH_OUTGOING = 1
    CALL_DIRECTION_WATCH_INCOMING_NORAML = 2
    CALL_DIRECTION_WATCH_INCOMING_MONITOR = 3
    
    CALL_ANSWERED = 1
    CALL_UNCONNECT = 2

    BATTERY_LEVEL_ALERT = 10
    LBS_MAX_DIFF_COUNT = 3

    NOTIFY_CODE_SET_SOS_OK = '10000'
    NOTIFY_CODE_SET_CLOCK_OK = '10001'
    NOTIFY_CODE_LOW_BATTERY = '10002'
    NOTIFY_CODE_NEW_SMS = '10003'
    NOTIFY_CODE_SOS_UNCONNECT = '10004'
    NOTIFY_CODE_LOCATION_OK = '20000'
    NOTIFY_CODE_LEAVE_FENCE = '20001'
    NOTIFY_CODE_INTO_FENCE = '20002'

    LBS_SERVICE_URL = "http://api.haoservice.com/api/viplbs"
    LBS_SERVICE_KEY = "05367aa46ebc40d2ae8e8ca9d082147c"
    

    LBS_SERVICE_AMAP_URL = "http://apilocate.amap.com/position?"
    LBS_SERVICE_AMAP_KEY = "f6eb6df47469c32387b9e607de31278e"

    LBS_Coordinate_GPS = 2
    LBS_Coordinate_BAIDU = 1
    LBS_Coordinate_Google = 0

    SMS_UserName = 'Wishoney'
    SMS_Password = '123456'
    SMS_Verifycode = 'abcdef'
    SMS_URL = 'http://120.24.238.189:8080/hawk_center/utils/smsagent.php?action=sendSms&account=%s&password=%s&extsubcode=866&phoneNumber=%s&content=%s'

    VID_ODA = '00000E004002'
    VID_WISHONEY = '00000E004001'
    VID_SMALLC = '00000E004005'


    PROTOCOL_VERSION_2 = '2.0'
    PROTOCOL_VERSION_1 = '1.0'

    JPUSH_APPKEY = "806dc45393137e69f7b20acb"
    JPUSH_SECRET = "396020c5252fb8138c79fb2e"

    def __init__(self):
        logger.debug("WorkerBase:__init__")
 
    def redisdelete(self, argslist):
        logger.debug('%s' % ('","'.join(argslist)))
        ret = eval('self._redis.delete("%s")'%('","'.join(argslist)))
        logger.debug('delete ret = %d' % (ret))

    def _sendMessage(self, to, body):
        #发送消息到routkey，没有返回reply_to,单向消息
#        logger.debug(to +':'+body)
        if to is None or to == '' or body is None or body == '':
            return

        self._redis.lpush(to, body)

