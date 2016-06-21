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
from databaseAPI.DatabaseInterface import DatabaseInterface
from databaseAPI.DatabaseUpdate import DatabaseUpdate

        
if __name__ == '__main__':

#    ex=DatabaseUpdate()
#    ex.update_stockinfo_basic()
def decoa(func):
    
    def a(*args,**kw):
        return len(func(*args,**kw))+2
        
    return a

def decob(func):
    def b(*args,**kw):
        return func(*args,**kw)[:2]
    return b

#myfunc=decob(myfunc)
#myfunc=decoa(decob(myfunc))=decoa(b)

@decoa
@decob
def myfunc():
    return [1,2,3]
    