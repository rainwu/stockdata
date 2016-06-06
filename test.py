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
import multiprocessing
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




def worker(t):
    
    print name, 'Starting'
    prc=StockDataProc()
    date='2016-06-03'
    trade_field=[['volume','amount'],['p_change']]
    itfs=[prc.wp.itfHDat_proc,prc.wp.itfHisDatD_proc]
    itfparas=[{'start':date,'end':date,'field': trade_field[0]},
                      {'start':date,'end':date,'field': trade_field[1]}]
    mergebys='date'
    data=prc._get_data(itfs,itfparas,mergebys,t)
    print data
    print name, 'Exiting'
    return data
    
    

prc=StockDataProc()

def _iter_func(keys,itfs,itfparas,mergebys):
    for k in keys:
        yield prc._get_data(itfs,itfparas,mergebys,k)
    
def iter_as_list(process_keys,itfs,itfparas,mergebys):
    gen_func=_iter_func(process_keys,itfs,itfparas,mergebys)
    return list(gen_func)
    
def iter_as_df(process_keys,itfs,itfparas,mergebys):
    gen_func=_iter_func(process_keys,itfs,itfparas,mergebys)
    return pd.concat(gen_func)
    
def iter_write(process_keys,itfs,itfparas,mergebys):
    pass
    
def process_func(process_keys):
    name = multiprocessing.current_process().name
    print name, 'Starting'
    date='2016-06-03'
    trade_field=[['volume','amount'],['p_change']]
    itfs=[prc.wp.itfHDat_proc,prc.wp.itfHisDatD_proc]
    itfparas=[{'start':date,'end':date,'field': trade_field[0]},
                        {'start':date,'end':date,'field': trade_field[1]}]
    mergebys='date'
    return iter_as_df(process_keys,itfs,itfparas,mergebys)
    
    
        
    #每个完成自己的快
        
if __name__ == '__main__':
#    jobs = []
#    p1 = multiprocessing.Process(name='first10',target=worker,args=('000001',))
#    p2 = multiprocessing.Process(name='last10',target=worker,args=('000002',))
#    jobs.append(p1)
#    jobs.append(p2)
#    p1.start()
#    p2.start()

    pool = Pool()
    ticker1=['000001','000002']
    ticker2=['601888','002707']
    result1 = pool.apply_async(process_func,[ticker1])
    result2 = pool.apply_async(process_func,[ticker2])
    answer1 = result1.get(timeout=100)
    answer2 = result2.get(timeout=100)
    print answer2
    print 'Main end'