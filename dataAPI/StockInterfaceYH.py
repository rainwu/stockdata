# -*- coding: utf-8 -*-
"""
Created on Sat Apr 02 15:54:34 2016

@author: saisn
"""
import os
os.chdir(os.environ["WORK_PATH"])
import settings
from Base import Base
from yahoo_finance import Share
import pandas as pd

class StockInterfaceYH(object):
    
    def __init__(self):
        self.ba=Base()
    
    #某一个股票的一段时间的日线
    def get_tradedata(self,ticker,start,end=''):
        if len(end)==0:
            end=self.ba.today_as_str()
        ticker_obj = Share(ticker)
        tradedata=ticker_obj.get_historical(start, end)
        return pd.DataFrame(tradedata)


