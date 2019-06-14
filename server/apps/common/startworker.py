#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:  asset_main.py
# creator:   jacob.qian
# datetime:  2013-5-31
# Ronaldo 启动workers的公共文件
import sys
import time
import json
import MySQLdb as mysql
import subprocess
import os
import logging
import logging.config
logging.config.fileConfig("/opt/Clousky/Ronaldo/server/conf/log.conf")
logb = logging.getLogger('ronaldo')
#import shlex


def startworker(moduleid):
    ''' parm1: moduleid,
    '''
    if moduleid is None:
        logb.error("Miss Parameter")
        sys.exit()
    projectdir = os.getcwd()
    logb.debug("projectdir = %s" % (projectdir))
#    moduleid = int(sys.argv[1])
    fileobj = open('/opt/Clousky/Ronaldo/server/conf/db.conf', 'r')
    json_dbcfg = json.load(fileobj)
    fileobj.close()
    conn = mysql.connect(host=json_dbcfg['host'], user=json_dbcfg['user'], passwd=json_dbcfg['passwd'], db=json_dbcfg['dbname'])
    cursor = conn.cursor(cursorclass=mysql.cursors.DictCursor)
    count = cursor.execute("select * from tbl_ronaldo_module_info where id = %d" % (moduleid))
    row = dict()
    if count:
        row = cursor.fetchone()
        cursor.close()
        conn.close()
    else:
        cursor.close()
        conn.close()
        logb.error("no rows match moduleid(%d)" % (moduleid))
        sys.exit()
    #启动工作线程
    for idx in range(0, row['workers']):
        cmdstr = ''
        if row['matchmode'] == 1:
            cmdstr = 'python '
            cmdstr += projectdir
            cmdstr += row['worker_cmd']
            cmdstr += " "
            cmdstr += str(row['id'])
#        else:
#            cmdstr += url

        logb.debug(cmdstr)
#        args = shlex.split(cmdstr)
#        print args
#        subprocess.Popen(args)
        subprocess.Popen(cmdstr, shell=True)
#        os.system(cmdstr)
    while True:
        time.sleep(10)
