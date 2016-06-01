# -*- coding: utf-8 -*-
"""
Created on Mon May 23 15:27:59 2016

@author: warriorzhai
"""

import os
try:
    os.chdir(os.environ["WORK_PATH"])
except KeyError:
    pass
from databaseAPI.DatabaseInit import DatabaseInit
from dataPROC.StockDataProc import StockDataProc
from dataPROC.StockDataStat import StockDataStat
from databaseAPI.DatabaseUpdate import DatabaseUpdate
from databaseAPI.DatabaseInterface import DatabaseInterface
from scheduler import Scheduler
import databaseAPI.db_tables as tables
import settings
import datetime
import pytz
import threading, time
from dateutil.tz import *
#
#timezone=settings.timezone
#
#def task():
#    up=DatabaseUpdate()
#    up.update_stockinfo_numerics()
#
##self.update_newtickers()
##        print '更新全部股票数值信息...'
##        self.update_stockinfo_numerics()
##        print '更新沪港通余额信息...'
##        self.insert_stockhgt()
#
#time=timezone.localize(datetime.datetime.now()+ datetime.timedelta(0,120))
#
#sd=Scheduler()
#taskline=sd.scheduler_set()
#sd.add_runattime(time,taskline,task)
#sd.start()
##执行语句


#多线程测试

def takeANap():
       time.sleep(30)
       up=DatabaseInterface()
       up.db_insertone({'name':'1605311'},'test')
       
def tz_tolocal(time,tz,timeformat='%Y-%m-%d %H:%M:%S'):
    if type(time)==str:
        time=datetime.datetime.strptime(time,timeformat)
    tz_time=tz.localize(time)
    return tz_time.astimezone(tzlocal())
    


threadObj = threading.Thread(target=takeANap)
threadObj.daemon = True
threadObj.Timer(delay, takeANap).start()



