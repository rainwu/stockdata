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
    
    
    #返回list
    def _get_grp(self,grpnam,grpsubnams):
        db_table=tables.stockgrps_table_struct
        collnam=db_table['collnam']
        filter_dic={ "grpnam":grpnam}   
        sel_fields=self.base.any_2list(grpsubnams)
        return self.dbobj.db_findone(filter_dic,sel_fields,collnam)[0]
    
    
    def get_tickerall(self):
        grpnam='tickers'
        grpsubnams=['tk_all']
        return self._get_grp(grpnam,grpsubnams)
    
    def get_tickerconcept(self,connam):
        grpnam='concepts'
        
        if type(connam)==str:
            connam=unicode(connam,encoding='utf-8')
            
        grpsubnams=[connam]
        return self._get_grp(grpnam,grpsubnams)
    
    #获取当日次新股列表
    def get_tickercxg(self,date):
        pass
    
    #获取当日开始打新的冻结资金
    def get_amtdx(self):
        pass
    
    #获取今日上市的新股
    def get_tickerxg(self):
        pass
    
    
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        