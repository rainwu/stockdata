# -*- coding: utf-8 -*-
"""
Created on Mon Apr 18 18:47:37 2016

@author: warriorzhai
"""
import os
try:
    os.chdir(os.environ["WORK_PATH"])
except KeyError:
    pass
import sys
import pymongo
import time
import databaseAPI.db_settings as settings
import databaseAPI.db_tables as tables
from Base import Base

use_sb='stkdb'

class DatabaseInterface(object):
    
    def __init__(self):
        self.db_url= settings.db_set[use_sb]
        self.base=Base()
    
    def _db_connect(self,op,op_paras={}):
#        for i in range(settings.try_times):
#            try:
#                res=op(**op_paras)
#            except:
#                print '连接出错，第'+str(i)+'重试中......'
#                time.sleep(settings.sleep_time)
#                if i==(settings.try_times-1):
#                    print '操作失败'
#                    sys.exit()
#                continue
#            break
        res=op(**op_paras)
        return res
    


    def db_connect(self):
        op=pymongo.MongoClient
        op_paras={'host':self.db_url}
        client = self._db_connect(op,op_paras)
        db = client[use_sb]
        return db
    
    def db_insertone(self,data,collnam):
        coll=self.db_connect()[collnam]
        op=coll.insert_one
        op_para={'document':data}
        try:
            result=self._db_connect(op,op_para)
        except pymongo.errors.DuplicateKeyError:
            print '表已存在项'
            return -1
        return result
    
    def db_insertmany(self,data,collnam):
        coll=self.db_connect()[collnam]
        op=coll.insert_many
        op_para={'documents':data}
        try:
            result=self._db_connect(op,op_para)
        except pymongo.errors.DuplicateKeyError:
            print '表已存在项'
            return -1
        return result
        
    def db_insertarray_one(self,filter_dic,update_dic,collnam):
        update_id='$addToSet'
        update={update_id:update_dic}
        result=self.db_updateone(filter_dic,update,collnam)
        return result
    
    def db_insertarray_many(self,filter_dic,update_dic,collnam):
        update_id='$addToSet'
        update_val_id='$each'
        update_val_keys=[update_val_id]*len(update_dic)
        update_val_vals=[self.base.any_2list(o) for o in update_dic.values()]
        update={update_id:self.base.lists_2dict(update_dic.keys(),self.base.lists_2dictlists(update_val_keys,update_val_vals))}
        result=self.db_updateone(filter_dic,update,collnam)
        return result
    
    
    def db_updateone(self,filter_dic,update_dic,collnam,upserts=True):
        coll=self.db_connect()[collnam]
        op=coll.update_one
        op_para={'filter':filter_dic,'update':update_dic}
        result=self._db_connect(op,op_para)
        return result
    
    def db_updatemany(self,filter_dic,update_dic,collnam,upserts=True):
        coll=self.db_connect()[collnam]
        op=coll.update_many
        op_para={'filter':filter_dic,'update':update_dic,'upserts':upserts}
        result=self._db_connect(op,op_para)
        return result
    
    
        
    def db_findone(self,filter_dic,collnam,sel_fields):
        coll=self.db_connect()[collnam]
        op=coll.find_one
        op_para={'filter':filter_dic}
        result=self._db_connect(op,op_para)
        if type(sel_fields)==str:
            return result[sel_fields]
        else:
            return [result[k] for k in sel_fields]
    
    def db_updateiter(self,filter_dicl,update_dicl,collnam):
        #配置控制记录
        ctrl_filter_dic={'collnam': collnam}
        ctrl_update_dic={'$inc': {'step': 1}}
        ctrl_updateover_dic={'$set': {'step': 0}}
        ctrl_findsel='step'
        #获取断点
        ctrl_table_struct=tables.control_table_struct
        step=self.db_findone(ctrl_filter_dic,ctrl_table_struct['collnam'],ctrl_findsel)
        print '断点开始于'+str(step)
        
        total_len=len(filter_dicl)-step
        process=0.0
        #批量更新
        for f,u in zip(filter_dicl[step:],update_dicl[step:]):
            print '更新数据.....'
            self.db_updateone(f,u,collnam)
            print '更新计数.....'
            self.db_updateone(ctrl_filter_dic,ctrl_update_dic,ctrl_table_struct['collnam'])
            process=process+1
            print '已完成'+str(round(process*100/total_len,2))+'%.....'
        print '数据更新完毕，重置计数器.....'
        self.db_updateone(ctrl_filter_dic,ctrl_updateover_dic,ctrl_table_struct['collnam'])
            
        return 0
    
    def db_count(self,collnam):
        coll=self.db_connect()[collnam]
        op=coll.count
        result=self._db_connect(op)
        return result
        
    def db_drop(self,collnam):
        coll=self.db_connect()[collnam]
        op=coll.drop
        result=self._db_connect(op)
        return result
    
    def db_ensure_index(self,collnam,indexnam,unique=True):
        coll=self.db_connect()[collnam]
        op=coll.ensure_index
        op_para={'key_or_list':indexnam,'unique':unique}
        result=self._db_connect(op,op_para)
        return result
        