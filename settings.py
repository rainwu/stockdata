# -*- coding: utf-8 -*-
"""
Created on Thu Nov 12 15:39:04 2015

@author: saisn
"""
home=False

import os
import tushare as ts
import pytz
if home:

    work_path='E:\gitmoney'
else:
    import ssl
    ssl._create_default_https_context = ssl._create_unverified_context
    work_path='C:\Users\warriorzhai\Desktop\mmDATA\gitver'
os.environ["WORK_PATH"] = work_path

token='af16a92da767cebb8f57df28c6b2f5ed7fc6e488e1fbc8647efc124fc8adb840'
ts.set_token(token)

try_times=5
sleep_time=5
timezone=pytz.timezone('Asia/Shanghai')
process_num=3
thread_num=2
date_format="%Y-%m-%d"
emptylist=[None,[],'',{}]
defaultna=None

