# -*- coding: utf-8 -*-
"""
Created on Thu Jun 16 15:21:35 2016

@author: warriorzhai
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 09:11:47 2016

@author: warriorzhai
"""

import os
try:
    os.chdir(os.environ["WORK_PATH"])
except KeyError:
    pass
from multiprocessing import JoinableQueue,Queue,Process
import multiprocessing
import threading
import logging

logger=logging.getLogger(__name__)
logger.setLevel(logging.INFO)# create a file handler
handler=logging.FileHandler('test.log')
handler.setLevel(logging.INFO)# create a logging format
formatter=logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)# add the handlers to the logger
logger.addHandler(handler)


class MultiProcessTask():
    
    def __init__(self,processnum=None,threadnum=2,thread_maxnum=4,funcdict={}):
        self.processnum=processnum if processnum else multiprocessing.cpu_count()
        self.threadnum=threadnum
        self.thread_maxnum=thread_maxnum
        self.funcdict=funcdict
        
    
    def _get_results(self,resultqueue):
        while not resultqueue.empty():
                yield resultqueue.get()
    
    def _put_tasks(self,tasks,taskqueue,taskqueue_lk):
        taskqueue_lk.acquire()
        try:
            #将数据塞入队列
            for i in tasks:
                taskqueue.put(i)
            taskqueue_lk.notify()
        finally:       
            taskqueue_lk.release()
    
    def _start_threads(self,taskqueue,taskqueue_lk,
                       task_funcsconst,task_funcsconstparas={},
                       resultqueue=None):
                           
        for p in range(self.threadnum):
            p = threading.Thread(target=self.multithread_task, args=(taskqueue,
                                         taskqueue_lk,task_funcsconst,
                                         task_funcsconstparas,resultqueue),
                                         name='TH'+str(p))
            p.daemon=True
            p.start()
        
    def _start_processes(self,taskqueue,taskqueue_lk,task_funcsconst,
                             task_funcsconstparas,resultqueue):
        for i in range(self.processnum):
            p = Process(target=self.multiprocess_task, args=(taskqueue,
                                         taskqueue_lk,
                                         task_funcsconst,task_funcsconstparas,
                                         resultqueue
                                         ),name='P'+str(i))
            p.daemon=True
            p.start()        
        
    def multiprocess_task(self,taskqueue,taskqueue_lk,
                          task_funcsconst,task_funcsconstparas={},
                          resultqueue=None):    
        #判断taskqueue是否为空的锁，因为queue的empty方法没有加锁，所以手工加锁
        taskqueue_lk_th = threading.Condition(threading.Lock())
        
        #等待taskqueue放入完毕
        taskqueue_lk.acquire()
        if taskqueue.empty():
             logger.info(multiprocessing.current_process().name+' wait!')
             taskqueue_lk.wait()
        taskqueue_lk.release()
        
        #开始线程，数据抓取的执行部分
        self._start_threads(taskqueue,taskqueue_lk_th,
                            task_funcsconst,task_funcsconstparas,
                            resultqueue)
            
        #进程等待所有线程结束
        logger.info(multiprocessing.current_process().name+' join!')
        taskqueue.join()
        logger.info(multiprocessing.current_process().name+' end!')
            
            
    def multithread_task(self,taskqueue,taskqueue_lk,
                         task_funcsconst=None,
                         task_funcsconstparas={},resultqueue=None):

        #执行数据抓取直到taskqueue所有task被执行完
        while 1:
            #taskqueue中没有任务时候，线程挂起
            #等待taskqueue放入完毕
            logger.info(threading.current_thread().name+' start!')
            taskqueue_lk.acquire()
            if taskqueue.empty():
                logger.info(threading.current_thread().name+' wait!')
                taskqueue_lk.wait()
            taskqueue_lk.release()
        
            #数据抓取，queue的get函数有锁
            if task_funcsconst is None:
                task_funcnam,task_funcparas=taskqueue.get()
                task_func=self.func_dict[task_funcnam]
            else:
                task_funcparas=taskqueue.get()
                task_func=task_funcsconst
                
            logger.info(threading.current_thread().name+' getdata!')
            task_funcparas.update(task_funcsconstparas)
            data=task_func(**task_funcparas)
            #数据写入，queue的put函数有锁
            logger.info(threading.current_thread().name+' writedata!')
            
            if not resultqueue is None: 
                resultqueue.put(data)
            #taskqueue的任务计数器减1，减到0释放上一级进程的join
            taskqueue.task_done()
        logger.info(threading.current_thread().name+' end!')
        
    
    def run_multiprocess(self,task_funcsiter=None,task_funcsiterparas={},
                            task_funcsconst=None,task_funcsconstparas={},
                            resultqueue=None):
        #任务传送queue
        taskqueue=JoinableQueue()
        #任务写入/唤醒lock
        taskqueue_lk = multiprocessing.Condition(multiprocessing.Lock())
        
        self._start_processes(taskqueue,taskqueue_lk,task_funcsconst,
                            task_funcsconstparas,resultqueue)
        #放入任务，唤醒进程
        if task_funcsconst is None:
            self._put_tasks(zip(task_funcsiter,task_funcsiterparas),taskqueue,taskqueue_lk)
        else:
            self._put_tasks(task_funcsiterparas,taskqueue,taskqueue_lk)
        logger.info('main join!')
        taskqueue.join()
        logger.info('main end!')
        
        return 
    
    
    def run_multithread(self,task_funcsiter=None,task_funcsiterparas={},
                            task_funcsconst=None,task_funcsconstparas={},
                            resultqueue=None):
                
        self.threadnum=min(len(task_funcsiter),self.thread_maxnum)
        #任务传送queue
        taskqueue=JoinableQueue()
        #任务写入/唤醒lock
        taskqueue_lk = threading.Condition(threading.Lock())
        
        self._start_threads(taskqueue,taskqueue_lk,
                            task_funcsconst,
                            task_funcsconstparas,resultqueue)
        #放入任务，唤醒进程
        if task_funcsconst is None:
            self._put_tasks(zip(task_funcsiter,task_funcsiterparas),taskqueue,taskqueue_lk)
        else:
            self._put_tasks(task_funcsiterparas,taskqueue,taskqueue_lk)
        
        taskqueue.join()

        return 
  
  
    def getdata_multiprocess(self,task_funcsiter=None,task_funcsiterparas={},
                            task_funcsconst=None,task_funcsconstparas={}):
        #结果传送queue
        resultqueue=Queue()
        
        self.run_multiprocess(task_funcsiter=task_funcsiter,
                              task_funcsiterparas=task_funcsiterparas,
                            task_funcsconst=task_funcsconst,
                            task_funcsconstparas=task_funcsconstparas,
                            resultqueue=resultqueue)
        
        return self._get_results(resultqueue)
    
    def getdata_multithread(self,task_funcsiter=None,task_funcsiterparas={},
                            task_funcsconst=None,task_funcsconstparas={}):
        #结果传送queue
        resultqueue=Queue()
        
        self.run_multithread(task_funcsiter=task_funcsiter,
                              task_funcsiterparas=task_funcsiterparas,
                            task_funcsconst=task_funcsconst,
                            task_funcsconstparas=task_funcsconstparas,
                            resultqueue=resultqueue)
        
        return self._get_results(resultqueue)
           