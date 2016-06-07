# -*- coding: utf-8 -*-
"""
Created on Mon Apr 25 11:23:06 2016

@author: warriorzhai
"""

import tushare as ts
import time
import datetime

from Base import Base
from multiprocessing import Pool
import multiprocessing as mp
import pandas as pd
from dataPROC.StockDataProc import StockDataProc





#pool = Pool()
#
#
#result1 = pool.apply_async(getdata,tickers[:10])
#result2 = pool.apply_async(getdata,tickers[10:])
#answer1 = result1.get(timeout=100)
#answer2 = result2.get(timeout=100)

#jobs = []
#ticker1=['000001','000002']
#ticker2=['601888','002707']
#
#        
#
#
#
#def getdata(t):
#    date='2016-06-03'
#    trade_field=[['volume','amount'],['p_change']]
#    itfs=[prc.wp.itfHDat_proc,prc.wp.itfHisDatD_proc]
#    itfparas=[{'start':date,'end':date,'field': trade_field[0]},
#                      {'start':date,'end':date,'field': trade_field[1]}]
#    mergebys='date'
#    name = multiprocessing.current_process().name
#    print name, 'Starting'
#    data=pd.concat(prc._get_data_iter(t,itfs,itfparas,mergebys))
#    data.to_csv(str(t[0])+'.csv',encoding='utf-8')
#    print name, 'Exiting'
#
#        
#
#p1 = multiprocessing.Process(name='first10',target=getdata, args=(ticker1,))
#p2 = multiprocessing.Process(name='last10',target=getdata, args=(ticker2,))
#jobs.append(p1)
#jobs.append(p2)
#p1.start()
#p2.start()



prc=StockDataProc()



def _iter_func(keys,itfs,itfparas,mergebys):
    for k in keys:
        yield prc._get_data(itfs,itfparas,mergebys,k)
            
def _divide_task(iterkeys,max_process):
            
    real_process=min(len(iterkeys),max_process)
            
    len_eachtask=len(iterkeys)/real_process
    len_bonustask=len(iterkeys)%real_process

    tasklist=[iterkeys[len_eachtask*i:len_eachtask*(i+1)] for i in range(real_process)]
    if len_bonustask>0:
        map(lambda x,i:tasklist[i].append(x),iterkeys[-len_bonustask:],
                         range(len_bonustask))
    return tasklist
        
#        assert _divide_task([1,2],4)==[[1],[2]]
#        assert _divide_task([1,2,3,4],4)==[[1],[2],[3],[4]]
#        assert _divide_task([1,2,3,4,5,6],4)==[[1,5],[2,6],[3],[4]]
        
            
        
def iter_as_list(process_keys,itfs,itfparas,mergebys):
    gen_func=_iter_func(process_keys,itfs,itfparas,mergebys)
    return list(gen_func)
            
def iter_as_df(process_keys,itfs,itfparas,mergebys):
    gen_func=_iter_func(process_keys,itfs,itfparas,mergebys)
    return pd.concat(gen_func)
            
def iter_write(process_keys,itfs,itfparas,mergebys):
    pass
        
        
def process_func(process_task,handledata_itf,itfs,itfparas,mergebys,result):
    name = mp.current_process().name
    print name, 'Starting'
    result.append(handledata_itf(process_task,itfs,itfparas,mergebys))
        



        
    #每个完成自己的快
        
if __name__ == '__main__':
#    jobs = []
#    p1 = multiprocessing.Process(name='first10',target=worker,args=('000001',))
#    p2 = multiprocessing.Process(name='last10',target=worker,args=('000002',))
#    jobs.append(p1)
#    jobs.append(p2)
#    p1.start()
#    p2.start()
    date='2016-06-03'
    iterkeys=['000001','000002','601888','002707','600585']
    trade_field=[['volume','amount'],['p_change']]
    getdata_itfs=[prc.wp.itfHDat_proc,prc.wp.itfHisDatD_proc]
    getdata_itfparas=[{'start':date,'end':date,'field': trade_field[0]},
                                {'start':date,'end':date,'field': trade_field[1]}]
    getdata_mergebys='date'
    handledata_index=0
    max_process=4
    manager = mp.Manager()
    
    handledata_itfs=[iter_as_df,iter_as_list,iter_write]
    handledata_itf=handledata_itfs[handledata_index]
        
    tasklists=_divide_task(iterkeys,max_process)
        
    #pool = mp.Pool()
    completed_task_pool=manager.list([])
    jobs=[]
    for process_task in tasklists:  
#        process = pool.apply_async(process_func,(process_task,handledata_itf,
#                                                 getdata_itfs,getdata_itfparas,
#                                                 getdata_mergebys))
        p=mp.Process(name=process_task[0],target=process_func,args=(process_task,handledata_itf,
                                                 getdata_itfs,getdata_itfparas,
                                                 getdata_mergebys,completed_task_pool))
        jobs.append(p)
        p.start()
    
    for proc in jobs:
        proc.join()
    
    print completed_task_pool
    

        
    
    #print completed_task_pool
#    pool = Pool()
#    ticker1=['000001','000002']
#    ticker2=['601888','002707']
#    result1 = pool.apply_async(process_func,[ticker1])
#    result2 = pool.apply_async(process_func,[ticker2])
#    answer1 = result1.get(timeout=100)
#    answer2 = result2.get(timeout=100)
#    print answer2
#    print 'Main end'