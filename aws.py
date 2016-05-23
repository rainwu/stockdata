# -*- coding: utf-8 -*-
"""
Created on Mon May 23 15:27:59 2016

@author: warriorzhai
"""


from databaseAPI.DatabaseInit import DatabaseInit
from dataPROC.StockDataProc import StockDataProc
from dataPROC.StockDataStat import StockDataStat
from databaseAPI.DatabaseUpdate import DatabaseUpdate
import databaseAPI.db_tables as tables

#执行语句
up=DatabaseUpdate()
up.update_stockinfo_numerics()