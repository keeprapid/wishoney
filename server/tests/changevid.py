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

imeis = sys.argv[1]
vid = sys.argv[2]


r = redis.StrictRedis(password='Kr123456')

KEY_TOKEN_NAME_ID = "W:tni:%s:%s:%s"
KEY_IMEI_ID = "W:g:%s:%s"
KEY_IMEI_ONLINE_FLAG = "W:g:o:%s"
KEY_CLIENT_ONLINE_FLAG = 'W:c:o:%s'
KEY_IMEI_CMD = 'W:im:%s:%s'
KEY_IMEI_LOCATION = 'W:il:%s'
KEY_PHONE_VERIFYCODE = 'W:vc:%s:%s'

imeilist = imeis.split(',')

for imei_str in imeilist:
    imei = int(imei_str)
    searchkey = KEY_IMEI_ID%(imei_str,'*')
    resultlist = r.keys(searchkey)
    if len(resultlist):
        imeikey = resultlist[0]
        r.hset(imeikey,'vid',vid)

    col.update({'imei':imei},{'$set':{'vid':vid}})
    print imei_str



