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
    did = dbgearinfo['_id'].__str__()

    imeikey = KEY_IMEI_ID%(imei,did)
    ownerid = ""
    follow = list()

    if 'follow' not in dbgearinfo:
        follow = list()
        ownerid = ""
    elif len(dbgearinfo['follow']) == 0:
        ownerid = ''
    else:
        follow = dbgearinfo['follow']
        ownerid = follow[0]

    print imei, follow, ownerid,imeikey
    r.hset(imeikey,'owner', ownerid)
    col.update({'imei':imei},{'$set':{'owner':ownerid}})
    for memberid in follow:
        print memberid
        try:
            memberinfo = mcol.find_one({'_id':ObjectId(memberid)})

        except Exception, e:
            print e.__class__, e.args
            continue

        if memberinfo == None:
            continue
        deviceinfo = memberinfo['device']
        if imeistr in deviceinfo:
            imeiinfo = deviceinfo[imeistr]
            if memberid == ownerid:
                imeiinfo['role'] = '1'
            else:
                imeiinfo['role'] = '0'
            print deviceinfo
            mcol.update({'_id':ObjectId(memberid)},{'$set':{'device':deviceinfo}})
            searchkey = KEY_TOKEN_NAME_ID % ('*','*',memberid)
            result = r.keys(searchkey)
            if len(result)>0:
                memberkey = result[0]
                print memberkey
                r.hset(memberkey,'device',deviceinfo)

    


print i


