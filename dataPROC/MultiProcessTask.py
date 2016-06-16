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
import multiprocessing as mp
import threading
import multiprocessing
from dataAPI.StockInterfaceWrap import StockInterfaceWrap



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
        try:
            #将数据塞入队列
            for i in tasks:
                self.taskqueue.put(i)
            self.taskqueue_lk.notify()
        finally:       
            self.taskqueue_lk.release()
    
    def multithread_task(self,taskqueue,resultqueue,taskqueue_lk_th,
                          task_func,task_funcparas):
        #执行数据抓取直到taskqueue所有task被执行完
        while 1:
            #taskqueue中没有任务时候，线程挂起
            taskqueue_lk_th.acquire()
            if taskqueue.empty():
                taskqueue_lk_th.wait()
            taskqueue_lk_th.release()
                 
            #数据抓取，queue的get函数有锁
            task_funcindex=taskqueue.get()
            data=task_func(task_funcindex,**task_funcparas)
            #数据写入，queue的put函数有锁
            resultqueue.put(data)
            #taskqueue的任务计数器减1，减到0释放上一级进程的join
            taskqueue.task_done()

        
    
    def multiprocess_task(self,taskqueue,resultqueue,taskqueue_lk,
                          thread_num,task_func,task_funcparas):    
        #判断taskqueue是否为空的锁，因为queue的empty方法没有加锁，所以手工加锁
        taskqueue_lk_th = threading.Condition(threading.Lock())
         
        #等待taskqueue放入完毕
        taskqueue_lk.acquire()
        if taskqueue.empty():
             taskqueue_lk.wait()
        taskqueue_lk.release()
        
        #开始线程，数据抓取的执行部分
        for th in range(thread_num):
            th = threading.Thread(target=self.multithread_task, args=(taskqueue,resultqueue,
                                     taskqueue_lk_th,task_func,task_funcparas),
                                     name=multiprocessing.current_process().name+str(th))
            th.daemon=True
            th.start()
            
        #进程等待所有线程结束
        taskqueue.join()

        
    
    def getdata(self,task_func,taskindexes,task_funcparas):
        #建立进程，在任务被放入完毕前，所有进程在等待状态
        self.start_processes(task_func,taskindexes,task_funcparas)
        #放入任务，唤醒进程
        self._put_tasks(taskindexes)
        self.taskqueue.join()
        return self._get_results()
        

class MultiThreadTask():
    
    def __init__(self,threadnum=None):
        self.threadnum=threadnum if threadnum else mp.cpu_count()
        #任务传送queue
        self.taskqueue=JoinableQueue()
        #任务写入/唤醒lock
        self.taskqueue_lk = multiprocessing.Condition(multiprocessing.Lock())
        #结果传送queue
        self.resultqueue=Queue()
    
    def start_threads(self,threadnum):
        for p in range(self.threadnum):
            p = Process(target=self.multithread_task, args=(self.taskqueue,self.resultqueue,
                                     self.taskqueue_lk))
            p.daemon=True
            p.start()
    
    def _get_results(self):
        while not self.resultqueue.empty():
                yield self.resultqueue.get()
        
            
    def _put_tasks(self,tasks):
        self.taskqueue_lk.acquire()
        try:
            #将数据塞入队列
            for i in tasks:
                self.taskqueue.put(i)
            self.taskqueue_lk.notify()
        finally:       
            self.taskqueue_lk.release()
    
    def multithread_task(self,taskqueue,resultqueue,taskqueue_lk):
        #执行数据抓取直到taskqueue所有task被执行完
        while 1:
            #taskqueue中没有任务时候，线程挂起
            #等待taskqueue放入完毕
            taskqueue_lk.acquire()
            if taskqueue.empty():
                taskqueue_lk.wait()
            taskqueue_lk.release()
        
            #数据抓取，queue的get函数有锁
            task_func,task_funcparas=taskqueue.get()
            data=task_func(**task_funcparas)
            #数据写入，queue的put函数有锁
            resultqueue.put(data)
            #taskqueue的任务计数器减1，减到0释放上一级进程的join
            taskqueue.task_done()
        
    
    def getdata(self,task_funcs,task_funcsparas):
        #建立进程，在任务被放入完毕前，所有进程在等待状态
        self.start_threads(len(task_funcs))
        #放入任务，唤醒进程
        self._put_tasks(zip(task_funcs,task_funcsparas))
        
        self.taskqueue.join()
        
        return self._get_results()
    
    
    

    


    
    

#
#logger.info('Hello baby')