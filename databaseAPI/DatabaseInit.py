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
import collections
import pymongo
from databaseAPI.DatabaseInterface import DatabaseInterface
from dataAPI.StockInterfaceWrap import StockInterfaceWrap
from dataPROC.StockDataProc import StockDataProc
from Base import Base 
import databaseAPI.db_tables as tables
from databaseAPI.DatabaseUpdate import DatabaseUpdate



class DatabaseInit(object):
    
    def __init__(self):
        self.dbobj=DatabaseInterface()
        self.proc=StockDataProc()
        self.wp=StockInterfaceWrap()
        self.base=Base()
        self.up=DatabaseUpdate()
        

    
    #如果每组是一列的df，则转为列表
    #返回查询列表和更细列表
    def hd_df2grp2lists(self,df,grpkey=0,drop_duplicate=True):
#        if df.shape[1]!=2:
#            print '列数必须为2'
#            sys.exit()
        
        if drop_duplicate:
            df=df.drop_duplicates()
        
        leftkeys=self.base.lists_minus(df.columns,self.base.any_2list(df.columns[grpkey]))
        grp=df.groupby(df.columns[grpkey])
        
        #为了断点续传不会乱序，使用排序字典
        grpdict=collections.OrderedDict(sorted(grp.groups.items()))
        
        keys=grpdict.keys()
        values=[]
        for j in leftkeys:
            val_ori=[df[j].ix[i].squeeze() for i in grpdict.values()]
            val=[v.tolist() if self.base.is_iter(v) else [v]  for v in val_ori]
            values.append(val)
        if len(values)==1:
            return (keys,values[0])
        else:
            return (keys,values)


    
    def hd_build_upparas_grp(self,df,grpkey=0):
        
        #数据查找和更新的字段名称
        filt_field=df.columns[grpkey]
        leftkeys=self.base.lists_minus(df.columns,self.base.any_2list(df.columns[grpkey]))
        update_field=df.columns[leftkeys]
        
        #数据处理，一对多的更新，将更新的内容合并成列表
        hd_method=self.hd_df2grp2lists
        filt_val_list,update_val_list=hd_method(df,grpkey)
        #构建更新语句
        
        filt_key_list=[filt_field]*len(filt_val_list)
        filt=self.base.lists_2dictlists(filt_key_list,filt_val_list)

        update_key_list=[update_field]*len(update_val_list)
        update_ori=self.base.lists_2dictlists(update_key_list,update_val_list)
        key_list=['$set']*len(update_ori)
        update=self.base.lists_2dictlists(key_list,update_ori)

        return (filt,update)
    
    #df--build values
    #itemnams--build keys
    def hd_build_inparas_grp(self,df,itemnams,grpkey=0):
        vals=self.hd_df2grp2lists(df,grpkey)
        insertdata=self.base.lists_todiclist(itemnams,vals)
        return insertdata
    
    #初始化一个表里所有的条目和其index值
    def init_table(self,tb,index_vals,index_id=0):
                #表名，表中每项定义，键值
        insertcollnam=tb['collnam']
        if self.dbobj.db_count(insertcollnam)>0:
            print '清空集合'
            self.base.db_drop(insertcollnam)
        self.up._insert_initwithindex(tb,index_vals,index_id)

        

            
    def insert_ctrl_one(self,tarcollnam,index_id=0):
        #数据库表定义
        table_struct=tables.control_table_struct
        #表名，表中每项定义，键值
        ctrlcollnam=table_struct['collnam']
        itemnams=table_struct['itemnams']
        itemvals=table_struct['itemvals']
        #ctrlcoll=self.dbobj.db_connect()[ctrlcollnam]
        
        #使用insert命令插入一条数据
        #构建insertone语句插入一个文件，格式：{itemnam1:value1,itemnam2:value2}
        insert_keys=itemnams
        #index必须给值
        index_val=tarcollnam
        insert_vals=self.base.any_2list(index_val)+itemvals
        insert_data=self.base.lists_2dict(insert_keys,insert_vals)
        #插入数据
        self.dbobj.db_insertone(insert_data,ctrlcollnam)
        self.dbobj.db_ensure_index(ctrlcollnam,itemnams[index_id],unique=True)
        
    
    def init_tableandcrtl(self,tb,index_vals,index_id=0):
        print '创建表....'
        self.init_table(tb,index_vals)
        print '创建控制表....' 
        self.insert_ctrl_one(tb['collnam'])
            
        
    
    def init_stockinfo(self):
        index_vals=self.dtproc.get_tickerall()
        table_struct=tables.stockinfo_table_struct
        return self.init_tableandcrtl(table_struct,index_vals)
    
    def init_stockgrps(self):
        table_struct=tables.stockgrps_table_struct
        index_vals=table_struct['indexvals']
        print '初始化grps表'
        return self.init_tableandcrtl(table_struct,index_vals)
    
    def init_stockhgt(self):
        table_struct=tables.stockhgt_table_struct
        self.init_stockhgt_update(table_struct['collnam'])
        
    
    def init_stockinfo_update(self,funcs):
        pass

    def init_stockgrps_update(self):
        pass

    def init_stockhgt_update(self,collnam):
        data=self.proc.get_dfc_hgt_day()
        insert_data=self.base.pd_df2diclist(data,index=True)
        self.dbobj.db_insertmany(insert_data,collnam)
    
    def update_stockgrps_tickers(self):
        self.up.insert_stockgrps_tickers()

    
    def update_stockgrps_concepts(self,db_table,crawl_field,crawl_field_2db,grpnam):   
        #build insert data
        #get data
        #------数据抓取------
        print '原始数据抓取....'
        crawl_data=self.wp.itfConCla_proc(field=crawl_field)
        #build 语句
        insert_keys,insert_vals=self.hd_df2grp2lists(crawl_data,grpkey=1)
        #插入数组数据
        print '插入组数据'+grpnam
        self.up.update_stockgrps(db_table,grpnam,insert_keys,insert_vals)
        
    
    def update_stockgrps_inds2(self,db_table,crawl_field,crawl_field_2db,grpnam):   
        #build insert data
        #get data
        #------数据抓取------
        print '原始数据抓取....'
        crawl_data=self.wp.itfEquInd_proc(field=crawl_field)
        #build 语句
        insert_keys,insert_vals=self.hd_df2grp2lists(crawl_data,grpkey=1)
        #插入数组数据
        print '插入组数据'+grpnam
        self.up.update_stockgrps(db_table,grpnam,insert_keys,insert_vals)
    
        
        

    def update_stockinfo_StoBas(self):
        self.up.update_stockinfo_basic()
    
    def update_stockinfo_EquInd(self):
        self.up.update_stockinfo_industry(lev=1)
        self.up.update_stockinfo_industry(lev=2)
    
    def update_stockinfo_ConCla(self):
        self.up.update_stockinfo_concept()
        

    


    
    
    
    
    
        