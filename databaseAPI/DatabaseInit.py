# -*- coding: utf-8 -*-
"""
Created on Mon Apr 18 19:41:16 2016

@author: warriorzhai
"""
import os
try:
    os.chdir(os.environ["WORK_PATH"])
except KeyError:
    pass
import sys
import pymongo
from databaseAPI.DatabaseInterface import DatabaseInterface
from dataAPI.StockInterfaceWrap import StockInterfaceWrap
from dataPROC.StockDataProc import StockDataProc
from Base import Base 
import databaseAPI.db_tables as tables



class DatabaseInit(object):
    
    def __init__(self):
        self.dbobj=DatabaseInterface()
        self.proc=StockDataProc()
        self.wp=StockInterfaceWrap()
        self.base=Base()
        
    def hd_df2dictlist(self,df,drop_duplicate=True):
        if drop_duplicate:
            df=df.drop_duplicates()
        return self.base.pd_df2diclist(df)
    
    #如果每组是一列的df，则转为列表
    #返回查询列表和更细列表
    def hd_df2grp2lists(self,df,grpkey=0,drop_duplicate=True):
        if df.shape[1]!=2:
            print '列数必须为2'
            sys.exit()
        
        if drop_duplicate:
            df=df.drop_duplicates()
            
        leftkey=1-grpkey
        grp=df.groupby(df.columns[grpkey])
        
        keys=grp.groups.keys()
        vals_ori=[df[[leftkey]].ix[i].squeeze() for i in grp.groups.values()]
        vals=[v.tolist() if self.base.is_iter(v) else [v] for v in vals_ori]
        return (keys,vals)

    #默认数据库查找索引的建立方式为hd_df2dictlist
    def hd_build_upparas(self,df,keyindex=0):
        hd_method=self.hd_df2dictlist
        #将索引值转化格式
        #索引可能是int或是list of int,为了便于处理，把int转为list
        keyindex=[keyindex] if type(keyindex)==int else keyindex
        leftindex=self.base.lists_minus(range(len(df.columns)),keyindex)
        #建立数据库查询字典列表
        filt=hd_method(df[keyindex])
        #建立数据库更新字典表
        
        update_ori=hd_method(df[leftindex])
        key_list=['$set']*len(update_ori)
        update=self.base.listpair_2dict(key_list,update_ori)
        return (filt,update)
    
    def hd_build_upparas_grp(self,df,keyindex=0):
        if df.shape[1]!=2:
            print '列数必须为2'
            sys.exit()
        
        #数据查找和更新的字段名称
        filt_field=df.columns[0]
        update_field=df.columns[1]
        
        #数据处理，一对多的更新，将更新的内容合并成列表
        hd_method=self.hd_df2grp2lists
        filt_val_list,update_val_list=hd_method(df)
        #构建更新语句
        
        filt_key_list=[filt_field]*len(filt_val_list)
        filt=update=self.base.listpair_2dict(filt_key_list,filt_val_list)

        update_key_list=[update_field]*len(update_val_list)
        update_ori=self.base.listpair_2dict(update_key_list,update_val_list)
        key_list=['$set']*len(update_ori)
        update=self.base.listpair_2dict(key_list,update_ori)

        return (filt,update)
        
    def init_table(self,collnam,itemnams,keyvals,key_index=0):
        coll=self.dbobj.db_connect()[collnam]
        #清空集合
        if self.dbobj.db_count(collnam)>0:
            print '清空集合'
            coll.drop()
        
        #集合长度
        items_len_n=len(itemnams)
        
        dict_table=[dict(zip(itemnams,[None]*key_index+[v]+[None]*(items_len_n-key_index-1))) \
            for v in keyvals]
        #写入数据库
        print '写入数据库'
        self.dbobj.db_insertmany(dict_table,collnam)
        coll.ensure_index(itemnams[key_index], unique=True)
        
            
    def insert_ctrl(self,tarcollnam):
        table_struct=tables.control_table_struct
        
        ctrlcollnam=table_struct['collnam']
        ctrlcoll=self.dbobj.db_connect()[ctrlcollnam]
        
        itemnams=table_struct['itemnams']
        keynam=itemnams[table_struct['keyindex']]
        itemvals=[tarcollnam,0,None]
        dict_table=dict(zip(itemnams,itemvals))
        
        if self.dbobj.db_findone({keynam:tarcollnam}):
            print 'control:表已存在项，重新初始化'
            self.dbobj.db_updateone({keynam:tarcollnam},dict_table,ctrlcollnam)
        else:
            self.dbobj.db_insertone(dict_table,ctrlcollnam)
        
        if self.dbobj.db_count(ctrlcollnam)==1:
            print 'control:key建立索引'
            ctrlcoll.ensure_index(itemnams[key_index], unique=True)
    
    def init_tableandcrtl(self,collnam,itemnams,keyvals,keyindex):
        print '创建表....'
        self.init_table(collnam,itemnams,keyvals,keyindex)
        print '创建控制表....' 
        self.insert_ctrl(collnam)
    
    def init_stockinfo(self):
        keyvals=self.dtproc.get_tickerall()
        table_struct=tables.stockinfo_table_struct
        table_struct['keyvals']=keyvals
        return self.init_tableandcrtl(**table_struct)

    def update_stockinfo_StoBas(self):
        
        #配置基本参数
        #------抓取参数表-----
        crawl_table=tables.stockinfo_crawlnew_struct
        #数据库参数表
        db_table=tables.stockinfo_table_struct
        
        #获取数据抓取field
        crawl_field=crawl_table['ts_StoBas']
        #数据抓取field对应的数据库field名称
        crawl_field_2db=crawl_table['db_StoBas']
        #获取数据库索引项名称，更新时按照此索引更新
        keyindex=db_table['keyindex']
        #获取需要更新的数据库的表名
        updatecollnam=db_table['collnam']
        
        print updatecollnam+':StoBas part 开始初次插入数据.....'
        #------数据抓取------
        
        print '原始数据抓取....'
        crawl_data=self.wp.itfStoBas_proc(field=crawl_field)

        #-------------------
        
        #------抓取数据处理------
        #转为数据库的项名
        crawl_data.columns=crawl_field_2db
        
        #------------------------

        #------数据批量更新------
        #建立批量数据更新语句
        df_proc=crawl_data
        db_filt_list,db_update_list=self.hd_build_upparas(df_proc,keyindex)
         #批量更新
        print '批量更新数据....'
        self.dbobj.db_updateiter(db_filt_list,db_update_list,updatecollnam)
        
        
#        filt_StoBas=self.hd_df2dictlist(data_StoBas[keyindex])
#        update_StoBas=self.hd_df2dictlist(data_StoBas[leftindex])
#        update_StoBas=[{x[0]:x[1]} for x in zip(['$set']*len(update_StoBas),update_StoBas)]
#        
        #批量更新
        
        

    
        
    def update_stockinfo_EquInd(self):
        #配置基本参数
        #------抓取参数表-----
        crawl_table=tables.stockinfo_crawlnew_struct
        #数据库参数表
        db_table=tables.stockinfo_table_struct
        
        #获取数据抓取field
        crawl_field=crawl_table['tl_EquInd']
        #数据抓取field对应的数据库field名称
        crawl_field_2db=crawl_table['db_EquInd']
        #获取数据库索引项名称
        keyindex=db_table['keyindex']
        #获取需要更新的数据库的表名
        updatecollnam=db_table['collnam']
        
        
        #------数据抓取------
        crawl_data=self.wp.itfEquInd_proc(field=crawl_field)
        #-------------------
        
        #------抓取数据处理------
        #转为数据库的项名
        crawl_data.columns=crawl_field_2db

        #------------------------

        #------数据批量更新------
        #建立批量数据更新语句
        df_proc=crawl_data[[0,1]]
        db_filt_list,db_update_list=self.hd_build_upparas_grp(df_proc,keyindex)
         #批量更新
        print '更新行业一'
        self.dbobj.db_updateiter(db_filt_list,db_update_list,updatecollnam)
        
        #建立批量数据更新语句
        df_proc=crawl_data[[0,2]]
        db_filt_list,db_update_list=self.hd_build_upparas_grp(df_proc,keyindex)
         #批量更新
        print '更新行业二'
        self.dbobj.db_updateiter(db_filt_list,db_update_list,updatecollnam)
        
    def update_stockinfo_ConCla(self):
        #配置基本参数
        #------抓取参数表-----
        crawl_table=tables.stockinfo_crawlnew_struct
        #数据库参数表
        db_table=tables.stockinfo_table_struct
        
        #获取数据抓取field
        crawl_field=crawl_table['ts_ConCla']
        #数据抓取field对应的数据库field名称
        crawl_field_2db=crawl_table['db_ConCla']
        #获取数据库索引项名称
        keyindex=db_table['keyindex']
        #获取需要更新的数据库的表名
        updatecollnam=db_table['collnam']
        
        
        #------数据抓取------
        crawl_data=self.wp.itfConCla_proc(field=crawl_field)
        #-------------------
        
        #------抓取数据处理------
        #转为数据库的项名
        crawl_data.columns=crawl_field_2db

        #------------------------

        #------数据批量更新------
        #建立批量数据更新语句
        df_proc=crawl_data[[0,1]]
        db_filt_list,db_update_list=self.hd_build_upparas_grp(df_proc,keyindex)
         #批量更新
        print '更新概念'
        self.dbobj.db_updateiter(db_filt_list,db_update_list,updatecollnam)

    
    
    
    
    
        