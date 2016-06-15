# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 09:11:47 2016

@author: warriorzhai
"""

import os
#try:
#    os.chdir(os.environ["WORK_PATH"])
#except KeyError:
#    pass
import logging
from multiprocessing import JoinableQueue,Queue,Process
import threading
from dataAPI.StockInterfaceWrap import StockInterfaceWrap
import time

import multiprocessing




logger=logging.getLogger(__name__)
logger.setLevel(logging.INFO)# create a file handler
handler=logging.FileHandler('hello.log')
handler.setLevel(logging.INFO)# create a logging format
formatter=logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)# add the handlers to the logger
logger.addHandler(handler)



    



#
def multithread_getdata(taskqueue,resultqueue,taskqueue_lk,
                         task_func,task_funcparas={}):
    logger.info(threading.current_thread().name+'start!')
    while 1:
        taskqueue_lk.acquire()
        if taskqueue.empty():
            logger.info(threading.current_thread().name+'wait!')
            taskqueue_lk.wait()
        taskqueue_lk.release()
        
        logger.info(threading.current_thread().name+'get data!')
        task_funcindex=taskqueue.get()
        data=task_func(task_funcindex,**task_funcparas)
        logger.info(threading.current_thread().name+'put data!'+ task_funcindex)
        resultqueue.put(data)
        taskqueue.task_done()
        logger.info(threading.current_thread().name+'end!')
        
        

def multiprocess_startthread(taskqueue,resultqueue,taskqueue_lk,
                             task_func,taskindexes,task_funcparas,thread_num):
                                
    logger.info(multiprocessing.current_process().name+'start!')
    
    taskqueue_lk_th = threading.Condition(threading.Lock())
     
    taskqueue_lk.acquire()
    if taskqueue.empty():
         logger.info(multiprocessing.current_process().name+'wait!')
         taskqueue_lk.wait()
    taskqueue_lk.release()
        
    for th in range(thread_num):
        th = threading.Thread(target=multithread_getdata, args=(taskqueue,resultqueue,taskqueue_lk_th,
                                 task_func,task_funcparas),
                                 name=multiprocessing.current_process().name+str(th))
        th.daemon=True
        th.start()
    
    logger.info(multiprocessing.current_process().name+'join!')
    taskqueue.join()

    
if __name__ == '__main__':    
    wp=StockInterfaceWrap()
    taskindexes=['000001','000002','000876','600313','002215','002223']
    task_funcparas={'start':'2016-06-01','end':'2016-06-05'}
    task_func=wp.itfHDat_proc
    process_num=2
    thread_num=2
    
    taskqueue=JoinableQueue()
    resultqueue=Queue()
    taskqueue_lk = multiprocessing.Condition(multiprocessing.Lock())

    #开始进程
    for p in range(process_num):
        p = Process(target=multiprocess_startthread, args=(taskqueue,resultqueue,
                                 taskqueue_lk,task_func,taskindexes,task_funcparas,thread_num))
        p.start()
    
    #将数据塞入队列
    taskqueue_lk.acquire()
    logger.info('main get lock!')
    for i in taskindexes:
        taskqueue.put(i)
    logger.info('main notify!')
    taskqueue_lk.notify()
    logger.info('main free lock!')
    taskqueue_lk.release()
    logger.info('main free lock!')
    
    

#
#logger.info('Hello baby')