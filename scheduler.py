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
import pymongo
import datetime
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

logging.basicConfig()

def my_job():
    data={'name':321}
    dbobj=DatabaseInterface()
    dbobj.db_insertone(data,'test')

def test():
    print 'xxxx'


colnam='schedulelog'
dbnam='stkdb'
client = pymongo.MongoClient(host=settings.db_set['stkdb'])

tzone= pytz.timezone('Asia/Shanghai')

#jobstores = {
#    'mongo': MongoDBJobStore(collection=colnam, database=dbnam, client=client),
#    'default': MemoryJobStore()
#}

jobstores = {
    'default': MongoDBJobStore(collection=colnam, database=dbnam, client=client)
}


executors = {

     'default': ThreadPoolExecutor(10),
    'processpool': ProcessPoolExecutor(3)

}
job_defaults = {
    'coalesce': False,
    'max_instances': 3
}
scheduler = BackgroundScheduler(jobstores=jobstores,executors=executors,
                                job_defaults=job_defaults, timezone=tzone)
scheduler.add_job(test, run_date=datetime.datetime(2016,5,26,16,40, 5),timezone=tzone)
print scheduler.get_jobs()
scheduler.start()