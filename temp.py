# -*- coding: utf-8 -*-
"""
Created on Tue Jun 14 10:59:30 2016

@author: warriorzhai
"""
import multiprocessing as mp
from multiprocessing import Pipe
from multiprocessing import Lock, BoundedSemaphore, Semaphore, Condition
import collections
import time
import threading
import logging
import os
os.chdir('C:\Users\warriorzhai\Desktop\mmDATA\gitver')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# create a file handler
handler = logging.FileHandler('hello.log')
handler.setLevel(logging.INFO)
# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(handler)

mybuff = collections.deque()
reader, writer = Pipe(duplex=False)

def bufftopipe(buff,writer):
    while 1:
        if not buff:
            logger.info('waiting....') 
            time.sleep(5)
            continue
        else:            
            obj = buff.popleft()
            logger.info(threading.current_thread().name+' get data:'+ str(obj))
            
        logger.info(threading.current_thread().name+' send data')
        writer.send(obj)

notempty = threading.Condition(threading.Lock())

#def testcondrec(notempty,li):
#    logger.info('rec start....')
#    notempty.acquire()
#    if len(li)==0:
#        logger.info('rec wait...') 
#        notempty.wait()
#    logger.info('get data'+str(li.pop())) 
#    notempty.release()
#    logger.info('rec end....')




def testcondput(notempty,li,val):
    logger.info('put start....'+str(val))
    notempty.acquire()
    li.append(val)
    logger.info('not release....'+str(val))
#    notempty.notify()
#    logger.info('put notify....')
    time.sleep(5)
    logger.info('still not release....'+str(val))
    notempty.release()
    logger.info('put end....'+str(val))


l=[]
thread1 = threading.Thread(
                target=testcondput,
                args=(notempty,l,1),
                name='Q1'
                )
thread1.daemon = True
thread1.start()
thread2 = threading.Thread(
                target=testcondput,
                args=(notempty,l,2),
                name='Q2'
                )
thread2.daemon = True
thread2.start()


sem = BoundedSemaphore(2)
if sem.acquire():
    thread1 = threading.Thread(
                target=bufftopipe,
                args=(mybuff,writer),
                name='Q1'
                )
    thread1.daemon = True
    thread1.start()
    logging.info(thread1.name+'start!')
else:
    logging.info('thread full!')

if sem.acquire():
    thread2 = threading.Thread(
                target=bufftopipe,
                args=(mybuff,writer),
                name='Q2'
                )
    thread2.daemon = True
    logging.info(thread2.name+'start!')
    thread2.start()
    
else:
    logging.info('thread full!')
    
if sem.acquire():
    thread3 = threading.Thread(
                target=bufftopipe,
                args=(mybuff,writer),
                name='Q2'
                )
    thread3.daemon = True
    thread3.start()
    logging.info(thread3.name+'start!')
else:
    logging.info('thread full!') 
    
    
logging.info('append to buff')
mybuff.append(1)
logging.info('append to buff')
mybuff.append(2)
logging.info('append to buff')
mybuff.append(3)


cond = Condition()

notempty = threading.Condition(threading.Lock())



