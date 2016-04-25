# -*- coding: utf-8 -*-
"""
Created on Mon Apr 25 13:26:52 2016

@author: warriorzhai
"""
import os
os.chdir(os.environ["WORK_PATH"])
from DatabaseInit import DatabaseInit

instance=DatabaseInit()
instance.update_stockinfo_StoBas()