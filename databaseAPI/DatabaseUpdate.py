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
from databaseAPI.DatabaseProc import DatabaseProc
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
        
    def _update_updateiter(self,db_table,crawl_data):
        #获取数据库索引项名称，更新时按照此索引更新
        keyindex=db_table['keyindex']
        #获取需要更新的数据库的表名
        updatecollnam=db_table['collnam']
        
        print updatecollnam+':StoBas part 开始初次插入数据.....'
        #------数据批量更新------
        #建立批量数据更新语句
        df_proc=crawl_data
        db_filt_list,db_update_list=self._update_build_paras(df_proc,keyindex)
         #批量更新
        print '批量更新数据....'
        self.dbobj.db_updateiter(db_filt_list,db_update_list,updatecollnam)
    
    def _insert_initwithindex(self,tb,index_vals,index_id=0):
         insertcollnam=tb['collnam']
         insertitemnams=tb['itemnams']
         insertitemvals=tb['itemvals']
         #使用insert命令插入若干数据
         #构建insertone语句插入若干文件，格式：[{itemnam1:value1,itemnam2:value2},{itemnam1:value3,itemnam2:value4}]
         insert_keys=[insertitemnams]*len(index_vals)
         index_vals=self.base.any_2list(index_vals)
         #index必须给值
         insert_vals=[[v]+insertitemvals for v in index_vals]
         insert_data=self.base.lists_2dictlists(insert_keys,insert_vals)
         #插入数据
         self.dbobj.db_insertmany(insert_data,insertcollnam)
         self.dbobj.db_ensure_index(insertcollnam,insertitemnams[index_id],unique=True)
         
        
    def get_newtickers(self):
        db_tickers=self.db_proc.get_tickerall()
        ts_tickers=self.proc.get_tickerall()
        new_tickers=self.base.lists_minus(ts_tickers,db_tickers)
        return new_tickers
    
    def insert_stockinfo_init(self,tickers):
        table_struct_stockinfo=tables.stockinfo_table_struct
        self._insert_initwithindex(table_struct_stockinfo,tickers)

        
    def insert_stockinfo_init_update(self,tickers):
        self.update_stockinfo_basic(tickers)
        self.update_stockinfo_industry(tickers)
        self.update_stockinfo_industry(tickers,lev=2)
        self.update_stockinfo_concept(tickers)
    
        
    
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
    
    def insert_stockgrps_tickers(self,tickers=''):
        db_table=tables.stockgrps_table_struct
        grpnam='tickers'
        #build insert data
        insert_keys=['tk_all','tk_sh','tk_sz','tk_cyb']
        
        if len(tickers)==0:
            print '原始数据抓取....'
            tickers=self.db_proc.get_tickerall()
        
        grps_val_tksh=self.proc.get_tickersh(tickers)
        grps_val_tksz=self.proc.get_tickersz(tickers)
        grps_val_tkcyb=self.proc.get_tickercyb(tickers)
        insert_vals=[tickers,grps_val_tksh,grps_val_tksz,grps_val_tkcyb]
        print '插入组数据'+grpnam
        #插入数组数据
        self._update_arrayinsert(db_table,grpnam,insert_keys,insert_vals)
        

    
    def update_stockinfo_numerics(self,tickers=''):
        #配置基本参数
        #------抓取参数表-----
        crawl_table=tables.stockinfo_crawldaily_struct
        #数据库参数表
        db_table=tables.stockinfo_table_struct
        
        #获取数据抓取field
        crawl_field=crawl_table['ts_StoBas']
        #数据抓取field对应的数据库field名称
        crawl_field_2db=crawl_table['db_StoBas']
        
        res_row_sel={crawl_field[0]:tickers}
        
        print updatecollnam+':StoBas part 开始初次插入数据.....'
        #------数据抓取------
        print '原始数据抓取....'
        crawl_data=self.wp.itfStoBas_proc(field=crawl_field,res_row_sel=res_row_sel)
        
        #------抓取数据处理------
        #转为数据库的项名
        crawl_data.columns=crawl_field_2db

        self._update_updateiter(db_table,crawl_data)
    
    def update_stockinfo_basic(self,tickers=''):
        #配置基本参数
        #------抓取参数表-----
        crawl_table=tables.stockinfo_crawlnew_struct
        #数据库参数表
        db_table=tables.stockinfo_table_struct
        #获取数据抓取field
        crawl_field=crawl_table['ts_StoBas']
        #数据抓取field对应的数据库field名称
        crawl_field_2db=crawl_table['db_StoBas']
        
        res_row_sel={crawl_field[0]:tickers}
        
        crawl_data=self.wp.itfStoBas_proc(field=crawl_field,res_row_sel=res_row_sel)
                #------抓取数据处理------
        #转为数据库的项名
        crawl_data.columns=crawl_field_2db
        self._update_updateiter(db_table,crawl_data)
    
    def update_stockinfo_industry(self,tickers='',lev=1):
        #配置基本参数
        #------抓取参数表-----
        crawl_table=tables.stockinfo_crawlnew_struct
        #数据库参数表
        db_table=tables.stockinfo_table_struct
        #获取数据抓取field
        crawl_field=crawl_table['tl_EquInd'][0]+crawl_table['tl_EquInd'][lev]
        #数据抓取field对应的数据库field名称
        crawl_field_2db=crawl_table['db_EquInd']
        
        res_row_sel={crawl_field[0]:tickers}
        
        crawl_data=self.wp.itfEquInd_proc(field=crawl_field,res_row_sel=res_row_sel)
                #------抓取数据处理------
        #转为数据库的项名
        crawl_data.columns=crawl_field_2db
        self._update_updateiter(db_table,crawl_data)

        

        
    def update_stockinfo_concept(self,tickers=''):
        #配置基本参数
        #数据库参数表
        db_table=tables.stockinfo_table_struct
        #------抓取参数表-----
        crawl_table=tables.stockinfo_crawlnew_struct
        #获取数据抓取field
        crawl_field=crawl_table['ts_ConCla']
        #数据抓取field对应的数据库field名称
        crawl_field_2db=crawl_table['db_ConCla']
        
        res_row_sel={crawl_field[0]:tickers}
        
        crawl_data=self.wp.itfConCla_proc(field=crawl_field,res_row_sel=res_row_sel)
        #转为数据库的项名
        crawl_data.columns=crawl_field_2db
        
        self._update_updateiter(db_table,crawl_data)
        
                          
    def update_newtickers(self):
        new_tickers=self.get_newtickers()
        if new_tickers:
            self.insert_stockinfo_init(new_tickers)
            self.insert_stockgrps_tickers()
            self.insert_stockinfo_init_update(new_tickers)
        else:
            print '没有新增ticker'
            return -1
    

    
    

        

    