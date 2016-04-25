# -*- coding: utf-8 -*-
"""
Created on Mon Apr 25 13:26:52 2016

@author: warriorzhai
"""
import os
try:
    os.chdir(os.environ["WORK_PATH"])
except KeyError:
    pass
from databaseAPI.DatabaseInit import DatabaseInit

instance=DatabaseInit()
#instance.insert_stockinfo_StoBas()
instance.update_stockinfo_EquInd()
#instance.insert_stockinfo_ConCla()