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
        
        #建立数据库查询字典列表，由于列表可能很长，改为iter
        filt=iter(hd_method(df[keyindex]))
        #建立数据库更新字典表，由于列表可能很长，改为iter
        update=iter(hd_method(df[leftindex]))
        
        return (filt,update)
    
    def _update_arrayinsert(self,db_table,filter_val,update_key,update_val):
        updatecollnam=db_table['collnam']
        indexnam=db_table['itemnams'][0]
        
        filter_dic={indexnam:filter_val}
        update_dic=self.base.lists_2dict(update_key,update_val)
        
        self.dbobj.db_insertarray_many(filter_dic,update_dic,updatecollnam)
        
    def _update_updateiter(self,db_table,crawl_data):
        
        if crawl_data.empty:
            print 'no data...pass...'
            return -1
        #获取数据库索引项名称，更新时按照此索引更新
        keyindex=db_table['keyindex']
        #获取需要更新的数据库的表名
        updatecollnam=db_table['collnam']
        
        print updatecollnam+':start updating data.....'
        #------数据批量更新------
        #建立批量数据更新语句
        df_proc=crawl_data
        db_filt_list,db_update_list=self._update_build_paras(df_proc,keyindex)
         #批量更新
        self.dbobj.db_updatemultiprocess(db_filt_list,db_update_list,updatecollnam)
        
    def _update_insertarriter(self,db_table,crawl_data):
        if crawl_data.empty:
            print '无数据...跳过...'
            return -1
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
        self.dbobj.db_updateiter(db_filt_list,db_update_list,updatecollnam,self.dbobj.db_insertarray_one)
        
        
    def _insert_many(self,df,collnam,index_id=0,useiter=False):
         if not df.index.name is None:
                df.reset_index(level=index_id, inplace=True)
                
         insert_data=self.base.pd_df2diclist(df)
         #插入数据
         if useiter:
             self.dbobj.db_insertmultiprocess(insert_data,collnam)
         else:
             self.dbobj.db_insertmany(insert_data,collnam)
         self.dbobj.db_ensure_index(collnam,df.columns[index_id],unique=True)

    
    def _insert_initwithindex(self,tb,index_vals,index_id=0):
         insertcollnam=tb['collnam']
         insertitemnams=tb['itemnams']
         insertitemvals=tb['itemvals']
         #使用insert命令插入若干数据
         #构建insertone语句插入若干文件，格式：[{itemnam1:value1,itemnam2:value2},{itemnam1:value3,itemnam2:value4}]

         index_vals=self.base.any_2list(index_vals)
         
         df=pd.DataFrame(index=index_vals,columns=insertitemnams[1:])
         df.index.name=insertitemnams[index_id]
         
         m=len(index_vals)
         
         for x,y in zip(df.columns,insertitemvals):
             df[x]=[y]*m
             
         self._insert_many(df,insertcollnam,index_id)
         
        
    def get_newtickers(self):
        db_tickers=self.db_proc.get_tickerall()
        ts_tickers=self.proc.get_tickerall()
        new_tickers=self.base.lists_minus(ts_tickers,db_tickers)
        return new_tickers
    
    def get_newhgtdate(self):
        hgt_perpage=30
        #计算需要更新的日期列表
        #取数据库中的最新日期
        db_table=tables.stockhgt_table_struct
        sel_fields='date'
        collnam=db_table['collnam']
        db_dates=self.dbobj.db_find(sel_fields,collnam)
        db_date_new=self.base.date_newest(db_dates)
        wb_date_new=self.base.datetime_to_str(self.proc.get_workdays_last())
        
        date_gap=self.base.date_minus(wb_date_new,db_date_new)
        
        if date_gap==0:
            print '数据已是最新....'
            return -1
        
        p_max=date_gap/hgt_perpage+1
        start=self.base.date_inc(db_date_new)

        return (start,wb_date_new,p_max)
    
    def insert_stockinfo_init(self,tickers):
        table_struct_stockinfo=tables.stockinfo_table_struct
        self._insert_initwithindex(table_struct_stockinfo,tickers)

        
    def insert_stockinfo_init_update(self,tickers):
        self.update_stockinfo_basic(tickers)
        self.update_stockinfo_industry(tickers)
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
            tickers=self.proc.get_tickerall()
        
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
        

        #------数据抓取------
        print 'capture all ticker basic info....'
        crawl_data=self.wp.itfStoBas_proc(field=crawl_field,res_row_sel=res_row_sel)
        
        #------抓取数据处理------
        #转为数据库的项名
        crawl_data.columns=crawl_field_2db
        
        #特殊处理，将日期由int格式转为标准str格式
        crawl_data['timeToMarket']=self.base.int_to_strdate(crawl_data['timeToMarket'])

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
    
    def update_stockinfo_industry(self,tickers=''):
        #配置基本参数
        #------抓取参数表-----
        crawl_table=tables.stockinfo_crawlnew_struct
        #数据库参数表
        db_table=tables.stockinfo_table_struct
        #获取数据抓取field
        crawl_field=crawl_table['tl_EquInd']
        #数据抓取field对应的数据库field名称
        crawl_field_2db=crawl_table['db_EquInd']
        
        res_row_sel={crawl_field[0]:tickers}
        
        crawl_data=self.wp.itfEquInd_proc(field=crawl_field,res_row_sel=res_row_sel)
                #------抓取数据处理------
        #转为数据库的项名
        crawl_data.columns=crawl_field_2db
        self._update_insertarriter(db_table,crawl_data)

        

        
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
        
        self._update_insertarriter(db_table,crawl_data)
    
    def insert_stockhgt(self):
        db_table=tables.stockhgt_table_struct
        gethgtinfo=self.get_newhgtdate()
        if gethgtinfo==-1:
            return -1
        else:
            start,end,p_max=gethgtinfo
        df=self.proc.get_dfc_hgt_day(start,end,p_max)
        
        self._insert_many(df,db_table['collnam'])
            
                          
    def update_newtickers(self):
        new_tickers=self.get_newtickers()
        if new_tickers:
            print '插入新股票，初始化....'
            self.insert_stockinfo_init(new_tickers)
            print '更新股票列表....'
            self.insert_stockgrps_tickers()
            print '更新新股票数据....'
            self.insert_stockinfo_init_update(new_tickers)
        else:
            print '没有新增ticker'
            return -1
            
    def update_daily(self):
        self.update_newtickers()
        print '更新全部股票数值信息...'
        self.update_stockinfo_numerics()
        print '更新沪港通余额信息...'
        self.insert_stockhgt()
    
    def update_monthly(self):
        self.update_stockinfo_basic()
    
#['300512', '603737', '601611', '300516', '300513']
    
#ex._insert_many(df.ix[:3],'test',useiter=True)

        

    