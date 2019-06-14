#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:   ipserver_main.py
# creator:   jack.li
# datetime:  2014-8-18
# Ronaldo  ip 主文件
import sys
if '/opt/Clousky/Ronaldo/server/apps/common' not in sys.path:
    sys.path.append('/opt/Clousky/Ronaldo/server/apps/common')
import startworker


if __name__ == "__main__":
    ''' parm1: moduleid,
    '''
    startworker.startworker(int(sys.argv[1]))
