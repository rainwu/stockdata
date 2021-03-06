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
import multiprocessing as mp
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


    

class MultiProcessTask():
    
    def __init__(self,processnum=None,threadnum=2):
        self.processnum=processnum if processnum else mp.cpu_count()
        self.threadnum=threadnum
        #任务传送queue
        self.taskqueue=JoinableQueue()
        #任务写入/唤醒lock
        self.taskqueue_lk = multiprocessing.Condition(multiprocessing.Lock())
        #结果传送queue
        self.resultqueue=Queue()
    
    def start_processes(self,task_func,taskindexes,task_funcparas):
        for p in range(self.processnum):
            p = Process(target=self.multiprocess_task, args=(self.taskqueue,self.resultqueue,
                                     self.taskqueue_lk,self.threadnum,task_func,task_funcparas))
            p.start()
    
    def _get_results(self):
        while not self.resultqueue.empty():
                yield self.resultqueue.get()
        
            
    def _put_tasks(self,tasks):
        self.taskqueue_lk.acquire()
        logger.info('main get lock!')
        try:
            #将数据塞入队列
            for i in tasks:
                self.taskqueue.put(i)
            logger.info('main notify!')
            self.taskqueue_lk.notify()
        finally:       
            logger.info('main free lock!')
            self.taskqueue_lk.release()
    
    def multithread_task(self,taskqueue,resultqueue,taskqueue_lk_th,
                          task_func,task_funcparas):
        logger.info(threading.current_thread().name+'start!')
        #执行数据抓取直到taskqueue所有task被执行完
        while 1:
            #taskqueue中没有任务时候，线程挂起
            taskqueue_lk_th.acquire()
            if taskqueue.empty():
                logger.info(threading.current_thread().name+'wait!')
                taskqueue_lk_th.wait()
            taskqueue_lk_th.release()
            
            
            logger.info(threading.current_thread().name+'get data!')
            #数据抓取，queue的get函数有锁
            task_funcindex=taskqueue.get()
            data=task_func(task_funcindex,**task_funcparas)
            logger.info(threading.current_thread().name+'put data!'+ task_funcindex)
            #数据写入，queue的put函数有锁
            resultqueue.put(data)
            #taskqueue的任务计数器减1，减到0释放上一级进程的join
            taskqueue.task_done()
            logger.info(threading.current_thread().name+'end!')
        
    
    def multiprocess_task(self,taskqueue,resultqueue,taskqueue_lk,
                          thread_num,task_func,task_funcparas):                 
        logger.info(multiprocessing.current_process().name+'start!')
        #判断taskqueue是否为空的锁，因为queue的empty方法没有加锁，所以手工加锁
        taskqueue_lk_th = threading.Condition(threading.Lock())
         
        #等待taskqueue放入完毕
        taskqueue_lk.acquire()
        if taskqueue.empty():
             logger.info(multiprocessing.current_process().name+'wait!')
             taskqueue_lk.wait()
        taskqueue_lk.release()
        
        #开始线程，数据抓取的执行部分
        for th in range(thread_num):
            th = threading.Thread(target=self.multithread_task, args=(taskqueue,resultqueue,
                                     taskqueue_lk_th,task_func,task_funcparas),
                                     name=multiprocessing.current_process().name+str(th))
            th.daemon=True
            th.start()
            
        logger.info(multiprocessing.current_process().name+'end!')
        #进程等待所有线程结束
        logger.info(multiprocessing.current_process().name+'join!')
        taskqueue.join()

        
    
    def getdata(self,task_func,taskindexes,task_funcparas):
        #建立进程，在任务被放入完毕前，所有进程在等待状态
        self.start_processes(task_func,taskindexes,task_funcparas)
        #放入任务，唤醒进程
        self._put_tasks(taskindexes)
        
        logger.info('main join!')
        self.taskqueue.join()
        logger.info('main end!')
        return self._get_results()
        


    
if __name__ == '__main__':    
    wp=StockInterfaceWrap()
    taskindexes=['000001','000002','000876','600313','002215','002223']
    task_funcparas={'start':'2016-06-01','end':'2016-06-05'}
    task_func=wp.itfHDat_proc
    process_num=2
    thread_num=2
    
    task=MultiProcessTask(processnum=process_num)
    
    res=task.getdata(task_func,taskindexes,task_funcparas)
    print list(res)
    
    
    
    
    

    


    
    

#
#logger.info('Hello baby')