# -*- coding: utf-8 -*-
"""
Created on Wed May 04 13:08:53 2016

@author: warriorzhai
"""

import os
try:
    os.chdir(os.environ["WORK_PATH"])
except KeyError:
    pass
import sys
import collections
import pymongo
from databaseAPI.DatabaseInterface import DatabaseInterface
from dataAPI.StockInterfaceWrap import StockInterfaceWrap
from dataPROC.StockDataProc import StockDataProc
from Base import Base 
import databaseAPI.db_tables as tables
from databaseAPI.DatabaseUpdate import DatabaseUpdate



class StockDataProcDB(object):
    
    def __init__(self):
        self.dbobj=DatabaseInterface()
        self.base=Base()
    
    def get_group_tickers(self,grpnam,cnam):
        collnam='stockgrps'
        filter_dic={'grpnam':grpnam}
        sel_fields=unicode(cnam,encoding='utf-8')
        return self.dbobj.db_findone(filter_dic,sel_fields,collnam)
    
    
    def get_tickers_info(self,tickers,sel_fields):
        collnam='stockinfo'
        filter_dic={'ticker':tickers}
        return pd.Dataframe(self.dbobj.db_findone(filter_dic,sel_fields,collnam))
    
    
    