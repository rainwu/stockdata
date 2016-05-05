# -*- coding: utf-8 -*-
"""
Created on Sat Apr 02 15:54:34 2016

@author: saisn
"""
import os
try:
    os.chdir(os.environ["WORK_PATH"])
except KeyError:
    pass
import settings
from Base import Base
from yahoo_finance import Share
import pandas as pd

#新兴市场股市指数MSCIEMF（yahoo:EEM），表现了新兴市场的股票市值情况
#https://www.msci.com/resources/factsheets/index_fact_sheet/msci-emerging-markets-index-usd-net.pdf


class StockInterfaceYH(object):
    
    def __init__(self):
        self.ba=Base()
    
    #某一个股票的一段时间的日线
    #返回内容：vAdj_Close	Close	Date	High	Low	Open	Symbol	Volum
    def get_tradedata(self,ticker,start,end=''):
        if len(end)==0:
            end=self.ba.today_as_str()
        ticker_obj = Share(ticker)
        tradedata=ticker_obj.get_historical(start, end)
        return pd.DataFrame(tradedata)


