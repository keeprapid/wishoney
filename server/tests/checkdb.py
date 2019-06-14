#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:   ipserver_main.py
# creator:   jack.li
# datetime:  2014-8-18
# Ronaldo  ip 主文件

import socket
import time
import threading
import sys
import redis
import json
import pymongo
from bson.objectid import ObjectId

conn = pymongo.Connection()
db = conn.gearcenter
col = db.gearinfo

mconn = pymongo.Connection()
mdb = mconn.member
mcol = mdb.memberinfo


r = redis.StrictRedis()

KEY_TOKEN_NAME_ID = "W:tni:%s:%s:%s"
KEY_IMEI_ID = "W:g:%s:%s"
KEY_IMEI_ONLINE_FLAG = "W:g:o:%s"
KEY_CLIENT_ONLINE_FLAG = 'W:c:o:%s'
KEY_IMEI_CMD = 'W:im:%s:%s'
KEY_IMEI_LOCATION = 'W:il:%s'
KEY_PHONE_VERIFYCODE = 'W:vc:%s:%s'

dbgears = col.find({})
i = 0
for dbgearinfo in dbgears:
    i+=1
    imei = dbgearinfo['imei']
    imeistr = str(imei)
    searchkey = KEY_IMEI_ID % (imeistr, '*')
    result = r.keys(searchkey)
    if len(result)>1:
        print imeistr,result



members = mcol.find({})
for member in members:
    i+=1
    searchkey = KEY_TOKEN_NAME_ID % ('*',member['username'], '*')
    result = r.keys(searchkey)
    if len(result)>1:
        print member['username'],result



