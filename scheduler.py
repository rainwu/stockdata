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
import settings as setting
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

logging.basicConfig(filename='/tmp/log', level=logging.DEBUG,
        format='[%(asctime)s]: %(levelname)s : %(message)s')

#数据库设定，任log存储
dbnam=settings.db_nam
colnam='schedulelog'
client = pymongo.MongoClient(host=settings.db_set['stkdb'])
            #scheduler设定
tzone= setting.timezone
            #任务记录存入mongo数据库
jobstores = {
                'default': MongoDBJobStore(collection=colnam, database=dbnam, client=client)
            }
            
executors = {
                # 'default': ThreadPoolExecutor(10),
                'default': ThreadPoolExecutor(10)#ProcessPoolExecutor(3)
            }
            
job_defaults = {
                'coalesce': False,
                'max_instances': 3
            }

class Scheduler(object):
    
        def __init__(self):
            self.base=Base()

        def scheduler_set(usestores=True,timezone=tzone,method=BackgroundScheduler):
            paras={'executors':executors,'job_defaults':job_defaults,'timezone':timezone}
            if usestores:
                paras['jobstores']=jobstores
            
            scheduler=method(**paras)
            return scheduler
        
        def add_runattime(time,scheduler,job_func,job_paras={},timeformat='%Y-%m-%d %H:%M:%S',
                          misfire_grace_time=300):
            #time=self.base.str_to_datetime(time,dateformat=timeformat)
            jobid=scheduler.add_job(job_func,run_date=time,misfire_grace_time=misfire_grace_time,
                              kwargs=job_paras)
            return jobid

dbnam=settings.db_nam
colnam='schedulelog'
client = pymongo.MongoClient(host=settings.db_set['stkdb'])
logging.basicConfig()
tzone= setting.timezone

jobstores = {
                'default': MongoDBJobStore(collection=colnam, database=dbnam, client=client)
            }
executors = {
                # 'default': ThreadPoolExecutor(10),
                'default': ThreadPoolExecutor(10)#ProcessPoolExecutor(3)
            }

job_defaults = {
                'coalesce': False,
                'max_instances': 3
            }
def tz_tolocal(time,tz,timeformat='%Y-%m-%d %H:%M:%S'):
    if type(time)==str:
        time=datetime.datetime.strptime(time,timeformat)
    try:
        tz_time=tz.localize(time)
    except ValueError:
        tz_time=time
    return tz_time.astimezone(tzlocal())
    
def takeANap():
       up=DatabaseInterface()
       up.db_insertone({'name':datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')},'test')

starttime=datetime.datetime.now(tzone)+datetime.timedelta(0,100)
starttime_local=tz_tolocal(starttime,tzone).replace(tzinfo=None)
t=datetime.datetime.now()+datetime.timedelta(0,100)
scheduler=BackgroundScheduler(executors=executors,jobstores=jobstores,job_defaults=job_defaults)
print 'job start at ',starttime,starttime_local,t
#scheduler.daemonic = False
#scheduler.remove_all_jobs()
print scheduler.add_job(takeANap,run_date=starttime_local,misfire_grace_time=180)
#scheduler.remove_all_jobs()
print scheduler.get_jobs()
scheduler.start()





time='2016-05-27 12:15:05'
usestores=True
timezone=tzone
paras={'executors':executors,'job_defaults':job_defaults,'timezone':timezone}
paras['jobstores']=jobstores
    
scheduler=BackgroundScheduler(**paras)
time=base.str_to_datetime(time,dateformat='%Y-%m-%d %H:%M:%S')
scheduler.add_job(test,run_date=time,timezone=timezone,misfire_grace_time=180)
scheduler.start()
#    
    