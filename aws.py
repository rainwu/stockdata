# -*- coding: utf-8 -*-
"""
Created on Mon May 23 15:27:59 2016

@author: warriorzhai
"""


from databaseAPI.DatabaseInit import DatabaseInit
from dataPROC.StockDataProc import StockDataProc
from dataPROC.StockDataStat import StockDataStat
from databaseAPI.DatabaseUpdate import DatabaseUpdate
from databaseAPI.DatabaseInterface import DatabaseInterface
#from scheduler import Scheduler
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
date='2016-06-02'
prc=StockDataProc()
res=prc.get_datetrade(date)
res.to_csv('20160602trade.csv',encoding='utf-8')



