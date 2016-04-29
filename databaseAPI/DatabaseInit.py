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
        
    def hd_df2dictlist(self,df,drop_duplicate=True):
        if drop_duplicate:
            df=df.drop_duplicates()
        
        return self.base.pd_df2diclist(df)
    
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
        update=self.base.lists_2dictlists(key_list,update_ori)
        return (filt,update)
    
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
        
    def insert_many(self,tb,index_vals,index_id=0):
        #表名，表中每项定义，键值
        insertcollnam=tb['collnam']
        insertitemnams=tb['itemnams']
        insertitemvals=tb['itemvals']
        
        if self.dbobj.db_count(insertcollnam)>0:
            print '清空集合'
            self.base.db_drop(insertcollnam)
        
        #使用insert命令插入若干数据
        #构建insertone语句插入若干文件，格式：[{itemnam1:value1,itemnam2:value2},{itemnam1:value3,itemnam2:value4}]
        insert_keys=[insertitemnams]*len(index_vals)
        index_vals=self.base.any_2list(index_vals)
        #index必须给值
        insert_vals=[[v]+insertitemvals for v in index_vals]
        
        insert_data=self.base.lists_2dictlists(insert_keys,insert_vals)
        #插入数据
        self.dbobj.db_insertmany(insert_data,insertcollnam)
        self.dbobj.db_ensure_index(insertitemnams[index_id],unique=True)
        

            
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
        self.dbobj.db_ensure_index(itemnams[index_id],unique=True)
        
    
    def init_tableandcrtl(self,tb,index_vals,index_id=0):
        print '创建表....'
        self.insert_many(tb,index_vals)
        print '创建控制表....' 
        self.insert_ctrl_one(tb['collnam'])
            
        
    
    def init_stockinfo(self):
        index_vals=self.dtproc.get_tickerall()
        table_struct=tables.stockinfo_table_struct
        return self.init_tableandcrtl(table_struct,index_vals)
    
    def init_stockgrps(self):
        table_struct=tables.stockgrps_table_struct
        index_vals=table_struct['index_vals']
        return self.init_tableandcrtl(table_struct,index_vals)
    
    def init_stockinfo_update(self,funcs):
        pass

    def init_stockgrps_update(self,funcs):
        pass
    
    def update_stockgrps_tickers(self,db_table,grpnam):
        
        #build insert data
        insert_keys=['tk_all','tk_sh','tk_sz','tk_cyb']
        grps_val_tkall=self.proc.get_tickerall()
        grps_val_tksh=self.proc.get_tickersh(grps_val_tkall)
        grps_val_tksz=self.proc.get_tickersz(grps_val_tkall)
        grps_val_tkcyb=self.proc.get_tickercyb(grps_val_tkall)
        insert_vals=[grps_val_tkall,grps_val_tksh,grps_val_tksz,grps_val_tkcyb]
        
        #插入数组数据
        self.up.update_stockgrps(db_table,grpnam,insert_keys,insert_vals)

    
    def update_stockgrps_concepts(self,db_table,crawl_field,crawl_field_2db,grpnam):   
        #build insert data
        #get data
        #------数据抓取------
        print '原始数据抓取....'
        crawl_data=self.wp.itfConCla_proc(field=crawl_field)
        #build 语句
        insert_keys,insert_vals=self.hd_df2grp2lists(crawl_data,grpkey=1)
        #插入数组数据
        self.up.update_stockgrps(db_table,grpnam,insert_keys,insert_vals)
        
        

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
        
        print updatecollnam+':EquInd part 开始初次插入数据.....'
        
        print '原始数据抓取....'
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
        
        
        print updatecollnam+':EquInd part 开始初次插入数据.....'
        #------数据抓取------
        print '原始数据抓取....'
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

    
    
    
    
    
        