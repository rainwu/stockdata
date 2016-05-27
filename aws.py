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
from Scheduler import Scheduler
import databaseAPI.db_tables as tables
import settings
import datetime
import pytz

timezone=settings.timezone

def task():
    up=DatabaseUpdate()
    up.update_stockinfo_numerics()

#self.update_newtickers()
#        print '更新全部股票数值信息...'
#        self.update_stockinfo_numerics()
#        print '更新沪港通余额信息...'
#        self.insert_stockhgt()

time=timezone.localize(datetime.datetime.now()+ datetime.timedelta(0,120))

sd=Scheduler()
taskline=sd.scheduler_set()
sd.add_runattime(time,taskline,task)
sd.start()
#执行语句
