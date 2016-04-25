# -*- coding: utf-8 -*-
"""
Created on Mon Apr 25 11:23:06 2016

@author: warriorzhai
"""

import tushare as ts
import time
import datetime
import os
os.chdir(os.environ["WORK_PATH"])
import settings
from Base import Base

print settings.date_format