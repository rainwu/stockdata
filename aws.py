# -*- coding: utf-8 -*-
"""
Created on Mon May 23 15:27:59 2016

@author: warriorzhai
"""


from databaseAPI.DatabaseInit import DatabaseInit
from dataPROC.StockDataProc import StockDataProc
from dataPROC.StockDataStat import StockDataStat
from databaseAPI.DatabaseUpdate import DatabaseUpdate
from Scheduler import Scheduler
import databaseAPI.db_tables as tables

up=DatabaseUpdate()
#self.update_newtickers()
#        print '更新全部股票数值信息...'
#        self.update_stockinfo_numerics()
#        print '更新沪港通余额信息...'
#        self.insert_stockhgt()

time='2016-05-27 14:00:00'

sd=Scheduler()
taskline=sd.scheduler_set()
sd.add_runattime(time,taskline,up.insert_stockhgt)
sd.start()
#执行语句
