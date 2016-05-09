# -*- coding: utf-8 -*-
"""
Created on Sat May 07 17:53:54 2016

@author: saisn
"""

import os
try:
    os.chdir(os.environ["WORK_PATH"])
except KeyError:
    pass
import settings
import pandas as pd
import pymongo
import databaseAPI.db_tables as tables
from Base import Base 
from dataAPI.StockInterfaceWrap import StockInterfaceWrap
from databaseAPI.DatabaseInterface import DatabaseInterface


class DatabaseProc(object):
    
    def __init__(self):
        self.dbobj=DatabaseInterface()
        self.wp=StockInterfaceWrap()
        self.base=Base()
    
    def get_tickers(self):
        db_table=stockgrps_table_struct
        collnam=db_table['collnam']
        filter_dic={ "grpnam": "tickers"}   
        sel_fields=['']
        db_findone(filter_dic,sel_fields,collnam)
        
        
        
        
        