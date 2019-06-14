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

conn = pymongo.Connection()
db = conn.gearcenter
col = db.gearinfo

r = redis.StrictRedis()



dbgears = col.find({})
i = 0
for dbgearinfo in dbgears:
	i+=1
	searchkey = "W:g:%s*"%(dbgearinfo['imei'])
	if len(r.keys(searchkey))== 0:
		print dbgearinfo.get('imei')
		insertkey = "W:g:%s:%s" %(dbgearinfo.get('imei'),dbgearinfo.get('_id').__str__())
		for key in dbgearinfo:
			if key in ['createtime', 'activetime','_id']:
				r.hset(insertkey,key,dbgearinfo[key].__str__())
			else:
				r.hset(insertkey,key,dbgearinfo[key])

		print r.hgetall(insertkey)



print i




