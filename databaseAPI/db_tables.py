# -*- coding: utf-8 -*-
"""
Created on Tue Apr 19 15:37:57 2016

@author: warriorzhai
"""

#control
control_table_struct={
    'collnam':'control',
    'itemnams':['collnam','step','update'],
    'keyindex':0
    }



#stockinfo
stockinfo_table_struct={
    'collnam':'stockinfo',
    'itemnams':['ticker','name','industryName1','industryName2',
    'concepts','area','outstanding', 'totals','timeToMarket'],
    'keyindex':0
    }
stockinfo_crawlnew_struct={
    'ts_StoBas':['code','name','area'],
    'db_StoBas':['ticker','name','area'],
    'ts_ConCla':['code','c_name'],
    'db_ConCla':['ticker','concepts'],
    'tl_EquInd':['ticker','industryName1','industryName2'],
    'db_EquInd':['ticker','industryName1','industryName2'],
}
stockinfo_crawldaily_struct={
    'ts_StoBas':['code','outstanding', 'totals','timeToMarket']
}