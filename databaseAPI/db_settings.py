# -*- coding: utf-8 -*-
"""
Created on Mon Apr 18 19:03:14 2016

@author: warriorzhai
"""
import os

#数据库设定
try_times=3
sleep_time=5
db_url='mongodb://%s:%s@ds011281.mlab.com:11281/%s'
db_set={}

#测试数据库
db_user='admin'
db_psw='admin'
db_nam='stkdb'
db_set[db_nam]=db_url % (db_user,db_psw,db_nam)
