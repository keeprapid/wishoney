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
import threading
import json
import logging
import logging.config
import redis
import random

if '/opt/Keeprapid/Wishoney/server/apps/common' not in sys.path:
    sys.path.append('/opt/Keeprapid/Wishoney/server/apps/common')
import workers

logging.config.fileConfig("/opt/Keeprapid/Wishoney/server/conf/log.conf")
logger = logging.getLogger('wishoney')


class LbsDog(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
#        super(LbsDog, self).__init__()
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("LbsDog :running in __init__")

        fileobj = open('/opt/Keeprapid/Wishoney/server/conf/db.conf', 'r')
        self._json_dbcfg = json.load(fileobj)
        fileobj.close()

        fileobj = open("/opt/Keeprapid/Wishoney/server/conf/config.conf", "r")
        self._config = json.load(fileobj)
        fileobj.close()
        self.thread_index = thread_index
        self.recv_queue_name = "W:Queue:LbsDog"
        if 'lbs_proxy' in _config:
            if 'Consumer_Queue_Name' in _config['lbs_proxy']:
                self.recv_queue_name = _config['lbs_proxy']['Consumer_Queue_Name']

        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])


    def run(self):
        logger.debug("Start LbsDog pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
        try:
#        if 1:
            while 1:
                key = "W:Queue:LbsProxy"
                keygoogle = "W:Queue:LbsGoogleProxy"
                count = self._redis.llen(key)
                if count>=5:
                    sendcontent = "WARNNING::::::::>Wishoney Lbs Queue is overlord, check it ASAP"
                    body = dict()
                    body['dest'] = "10614126@qq.com;838333180@qq.com"
                    body['content'] = sendcontent
                    body['carrier'] = 'email'
                    body['notify_type'] = self.NOTIFY_TYPE_WARNNING
                    body['email_from'] = "Wishoney<noreply@wishoney.com>"
                    body['email_subject'] = "WARNNING: LBS Error"
                    action = dict()
                    action['body'] = body
                    action['version'] = '1.0'
                    action['action_cmd'] = 'send_email_notify'
                    action['seq_id'] = '%d' % random.randint(0,10000)
                    action['from'] = ''
                    self._sendMessage(self._config['notify']['Consumer_Queue_Name'], json.dumps(action))
                # count = self._redis.llen(keygoogle)
                # if count>=5:
                #     sendcontent = "WARNNING::::::::>Wishoney Lbs google Queue is overlord, check it ASAP"
                #     body = dict()
                #     body['dest'] = "10614126@qq.com,838333180@qq.com"
                #     body['content'] = sendcontent
                #     body['carrier'] = 'email'
                #     body['notify_type'] = self.NOTIFY_TYPE_WARNNING
                #     body['email_from'] = "Wishoney<noreply@wishoney.com>"
                #     body['email_subject'] = "WARNNING: LBS Google Error"
                #     action = dict()
                #     action['body'] = body
                #     action['version'] = '1.0'
                #     action['action_cmd'] = 'send_email_notify'
                #     action['seq_id'] = '%d' % random.randint(0,10000)
                #     action['from'] = ''
                #     self._sendMessage(self._config['notify']['Consumer_Queue_Name'], json.dumps(action))

                time.sleep(30)

                    
        except Exception as e:
            logger.debug("%s except raised : %s " % (e.__class__, e.args))



if __name__ == "__main__":
    fileobj = open("/opt/Keeprapid/Wishoney/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()

    thread_count = 1
    if _config is not None and 'lbs_dog' in _config and _config['lbs_dog'] is not None:
        if 'thread_count' in _config['lbs_dog'] and _config['lbs_dog']['thread_count'] is not None:
            thread_count = int(_config['lbs_dog']['thread_count'])

    for i in xrange(0, thread_count):
        process = LbsDog(i)
        process.setDaemon(True)
        process.start()

    while 1:
        time.sleep(1)
