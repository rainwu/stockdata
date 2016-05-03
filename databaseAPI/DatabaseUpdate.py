# -*- coding: utf-8 -*-
"""
Created on Wed Apr 20 11:01:14 2016

@author: warriorzhai
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


class DatabaseUpdate(object):
    
    def __init__(self):
        self.dbobj=DatabaseInterface()
        self.wp=StockInterfaceWrap()
        self.base=Base()
    
    def update_stockgrps(self,db_table,grpnam,insert_keys,insert_vals):
        insertcollnam=db_table['collnam']
        indexnam=db_table['itemnams'][0]
        
        #构建数组插入语句 filt：{k:v} insert:{conceptnam:list}
        #build filt 
        filter_dic={indexnam:grpnam}
        
        #build insert data      
        update_dic=self.base.lists_2dict(insert_keys,insert_vals)
        
        #插入数组
        self.dbobj.db_insertarray_many(filter_dic,update_dic,insertcollnam)
    
    

        

    