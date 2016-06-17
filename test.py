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


        
if __name__ == '__main__':
    proc=StockDataProc()

    res=proc.test()
    
    print res