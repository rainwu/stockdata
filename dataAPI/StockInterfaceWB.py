# -*- coding: utf-8 -*-
"""
Created on Fri May 06 09:55:57 2016

@author: warriorzhai
"""

import tushare as ts
import time
import re
import datetime
import urllib2
import pandas as pd
import os
try:
    os.chdir(os.environ["WORK_PATH"])
except KeyError:
    pass
import settings
from Base import Base

date_format=settings.date_format

class StockInterfaceWB(object):
    
    def __init__(self):
        self.base=Base()
    
    def _get_web(self,url):
        #请求获取数据
        #如果连接失败：
        #休息sleep_time秒，重复请求settings.try_times次
        for i in range(settings.try_times):
            try:
                response = urllib2.urlopen(url)
                data=response.read()
            except:
                print '连接出错，第'+str(i)+'重试中......'
                time.sleep(settings.sleep_time)
                continue
            break
#        
        return data
    
    def _get_webs_generator(self,urls):
        for url in urls:
            yield self._get_web(url)
    
    
    #获取东方财富沪股通的每日交易数据
    def get_dfcf_hgt(self,p_max=12):
        url1='http://datainterface.eastmoney.com/EM_DataCenter/JS.aspx?type=SHT&sty=SHTHPS&st=2&sr=-1&p=%s'
        url2='&ps=30&js=var%20TEENQUBh={pages:(pc),data:[(x)]}&mkt=1'
        urls1=self.base.strlist_build(url1,range(1,p_max+1))
        urls=[url+url2 for url in urls1]
        
        data_list=[]
        df_titles=['date','unknown','buy_amt','sell_amt','buy_net_amt','bal_today','bal_total','head_nam',
                   'head_change','head_tik','sh','sh_change']
        for pg in self._get_webs_generator(urls):
            pg_data=[line.split(",") for line in re.findall('"(.+?)"',pg)]
            data_list=data_list+pg_data
        return pd.DataFrame(data_list,columns=df_titles)
        
        
        
        
        
        
        
        
        
        
        
        