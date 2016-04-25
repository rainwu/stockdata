# -*- coding: utf-8 -*-
"""
Created on Wed Apr 20 11:01:14 2016

@author: warriorzhai
"""

import os
os.chdir(os.environ["WORK_PATH"])
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
    
    

        

    