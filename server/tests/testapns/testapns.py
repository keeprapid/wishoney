#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:   ipserver_main.py
# creator:   jack.li
# datetime:  2014-8-18
# Ronaldo  ip 主文件
import OpenSSL
OpenSSL.SSL.SSLv3_METHOD = OpenSSL.SSL.TLSv1_METHOD
from apnsclient import *
import sys
'''
       - push_sandbox -- ``("gateway.sandbox.push.apple.com", 2195)``, the default.
        - push_production -- ``("gateway.push.apple.com", 2195)``
        - feedback_sandbox -- ``("feedback.sandbox.push.apple.com", 2196)``
        - feedback_production -- ``("feedback.push.apple.com", 2196)``
        '''


filename = sys.argv[2]
apnstype = sys.argv[1]
token = sys.argv[3]
print apnstype, filename

session = Session()
con = session.get_connection(apnstype,cert_file=filename)

message = Message(['1a51b56e100a69ec4268a5d2ba0161554d00f6e5927c422eb8404626a7b27707',], alert={"loc-key": "ANPS_20001"}, badge=0, sound="alarm.caf")
srv = APNs(con)
res = srv.send(message)

for token, reason in res.failed.items():
    code, errmsg = reason
    # according to APNs protocol the token reported here
    # is garbage (invalid or empty), stop using and remove it.
    print "Device failed: {0}, reason: {1}".format(token, errmsg)

# Check failures not related to devices.
for code, errmsg in res.errors:
    print "Error: {}".format(errmsg)

# Check if there are tokens that can be retried
if res.needs_retry():
    # repeat with retry_message or reschedule your task
    retry_message = res.retry()