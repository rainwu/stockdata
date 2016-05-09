# -*- coding: utf-8 -*-
"""
Created on Wed Apr 20 11:01:14 2016

@author: warriorzhai
"""

import os
try:
    os.chdir(os.environ["WORK_PATH"])
except KeyError:
    pass
import settings
import pandas as pd
import pymongo
import databaseAPI.db_tables as tables
from Base import Base 
from dataAPI.StockInterfaceWrap import StockInterfaceWrap
from dataPROC.StockDataProc import StockDataProc
from dataPROC.DatabaseProc import DatabaseProc
from databaseAPI.DatabaseInterface import DatabaseInterface



class DatabaseUpdate(object):
    
    def __init__(self):
        self.dbobj=DatabaseInterface()
        self.wp=StockInterfaceWrap()
        self.proc=StockDataProc()
        self.db_proc=DatabaseProc()
        self.base=Base()
    
    
    #默认数据库语句的建立方式为pd_df2diclist
    def _update_build_paras(self,df,keyindex=0):
        
        hd_method=self.base.pd_df2diclist
        #将索引值转化格式
        #索引可能是int或是list of int,为了便于处理，把int转为list
        keyindex=self.base.any_2list(keyindex)
        leftindex=self.base.lists_minus(range(len(df.columns)),keyindex)
        
        #建立数据库查询字典列表
        filt=hd_method(df[keyindex])
        #建立数据库更新字典表
        update=hd_method(df[leftindex])
        
        return (filt,update)
    
    def _update_arrayinsert(self,db_table,filter_val,update_key,update_val):
        updatecollnam=db_table['collnam']
        indexnam=db_table['itemnams'][0]
        
        filter_dic={indexnam:filter_val}
        update_dic=self.base.lists_2dict(update_key,update_val)
        
        self.dbobj.db_insertarray_many(filter_dic,update_dic,updatecollnam)
        
    def _update_updateiter(self,db_table,crawl_field,crawl_field_2db,
                           crawl_itf,crawl_itf_paras={}):
        #获取数据库索引项名称，更新时按照此索引更新
        keyindex=db_table['keyindex']
        #获取需要更新的数据库的表名
        updatecollnam=db_table['collnam']
        
        print updatecollnam+':StoBas part 开始初次插入数据.....'
        #------数据抓取------
        print '原始数据抓取....'
        crawl_data=crawl_itf(**crawl_itf_paras)
        
        #------抓取数据处理------
        #转为数据库的项名
        crawl_data.columns=crawl_field_2db

        #------数据批量更新------
        #建立批量数据更新语句
        df_proc=crawl_data
        db_filt_list,db_update_list=self._update_build_paras(df_proc,keyindex)
         #批量更新
        print '批量更新数据....'
        self.dbobj.db_updateiter(db_filt_list,db_update_list,updatecollnam)
        
    def get_newtickers(self):
        db_tickers=self.db_proc.get_tickerall()
        ts_tickers=self.proc.get_tickerall()
        new_tickers=self.base.lists_minus(ts_tickers,db_tickers)
        return new_tickers
        
    
    def insert_stockinfo_conceptArr(self,ticker,add_concepts):
        db_table=tables.stockinfo_table_struct
        update_key='concepts'
        filter_val=ticker
        update_val=add_concepts
        
        self._update_arrayinsert(db_table,filter_val,update_key,update_val)
    
    
    def insert_stockinfo_industryArr(self,industry_lev,ticker,add_industries):
        db_table=tables.stockinfo_table_struct
        update_key='industryName'+str(industry_lev)
        filter_val=ticker
        update_val=add_industries
        
        self._update_arrayinsert(db_table,filter_val,update_key,update_val)
        
    def update_stockgrps_Arr(self,db_table,filter_val,update_key,update_val):
        db_table=tables.stockgrps_table_struct
        self._update_arrayinsert(db_table,filter_val,update_key,update_val)

    
    def update_stockinfo_numerics(self):
        #配置基本参数
        #------抓取参数表-----
        crawl_table=tables.stockinfo_crawldaily_struct
        #数据库参数表
        db_table=tables.stockinfo_table_struct
        #获取数据抓取field
        crawl_field=crawl_table['ts_StoBas']
        #数据抓取field对应的数据库field名称
        crawl_field_2db=crawl_table['db_StoBas']

        crawl_itf=self.wp.itfStoBas_proc
        crawl_itf_paras={'field':crawl_field}

        self._update_updateiter(db_table,crawl_field,crawl_field_2db,
                           crawl_itf,crawl_itf_paras)
    
    
    def update_stockinfo_industry(self,data):
        #配置基本参数
        #------抓取参数表-----
        crawl_table=tables.stockinfo_crawlnew_struct
        #数据库参数表
        db_table=tables.stockinfo_table_struct

        self._update_updateiter(db_table,data)
        

        
    def update_stockinfo_concept(self):
        #配置基本参数
        #------抓取参数表-----
        crawl_table=tables.stockinfo_crawlnew_struct
        #数据库参数表
        db_table=tables.stockinfo_table_struct
        
        #获取数据抓取field
        crawl_field=crawl_table['ts_ConCla']
        #数据抓取field对应的数据库field名称
        crawl_field_2db=crawl_table['db_ConCla']
        
        crawl_itf=self.wp.itfConCla_proc
        crawl_itf_paras={'field':crawl_field}

        self._update_updateiter(db_table,crawl_field,crawl_field_2db,
                           crawl_itf,crawl_itf_paras)
                          
    def update_newtickers(self):
        new_tickers=self. get_newtickers()
        if new_tickers:
            
        else:
            print '没有新增ticker'
            return -1
    

    
    

        

    