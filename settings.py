# -*- coding: utf-8 -*-
"""
Created on Thu Nov 12 15:39:04 2015

@author: saisn
"""
import os
import tushare as ts


work_path='C:\Users\warriorzhai\Desktop\mmDATA'
os.environ["WORK_PATH"] = work_path

token='8fdc3afafa5d9b8ed5ef1114454328faae1aef0fe39ff4b7989a5b9c9ed343ae'
ts.set_token(token)

try_times=5
sleep_time=5

date_format="%Y-%m-%d"