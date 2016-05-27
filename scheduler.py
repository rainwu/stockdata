# -*- coding: utf-8 -*-
"""
Created on Thu May 26 14:50:57 2016

@author: warriorzhai
"""
import os
try:
    os.chdir(os.environ["WORK_PATH"])
except KeyError:
    pass
import pytz
import databaseAPI.db_settings as settings
from databaseAPI.DatabaseInterface import DatabaseInterface
from Base import Base 
import pymongo
import datetime
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

logging.basicConfig()

#数据库设定，任log存储
dbnam=settings.db_nam
colnam='schedulelog'
client = pymongo.MongoClient(host=settings.db_set['stkdb'])
            #scheduler设定
tzone= pytz.timezone('Asia/Shanghai')
            #任务记录存入mongo数据库
jobstores = {
                'default': MongoDBJobStore(collection=colnam, database=dbnam, client=client)
            }
            
executors = {
                # 'default': ThreadPoolExecutor(10),
                'default': ProcessPoolExecutor(3)
            }
            
job_defaults = {
                'coalesce': False,
                'max_instances': 3
            }

class Scheduler(object):
    
        def __init__(self):
            self.base=Base()

        def scheduler_set(self,usestores=True,timezone=tzone,method=BackgroundScheduler):
            paras={'executors':executors,'job_defaults':job_defaults,'timezone':timezone}
            if usestores:
                paras['jobstores']=jobstores
            
            scheduler=method(**paras)
            return scheduler
        
        def add_runattime(self,time,scheduler,job_func,job_paras={},timeformat='%Y-%m-%d %H:%M:%S',
                          misfire_grace_time=300):
            time=self.base.str_to_datetime(time,dateformat=timeformat)
            scheduler.add_job(job_func,run_date=time,misfire_grace_time=misfire_grace_time,
                              kwargs=job_paras)
            return scheduler


    
#scheduler=scheduler_set()
#add_runattime('2016-05-27 11:58:00',scheduler,test)
#print scheduler.get_jobs()
#scheduler.start()
#time='2016-05-27 12:15:05'
#usestores=True
#timezone=tzone
#paras={'executors':executors,'job_defaults':job_defaults,'timezone':timezone}
#paras['jobstores']=jobstores
#    
#scheduler=BackgroundScheduler(**paras)
#time=base.str_to_datetime(time,dateformat='%Y-%m-%d %H:%M:%S')
#scheduler.add_job(test,run_date=time,timezone=timezone,misfire_grace_time=180)
#scheduler.start()
#    
    