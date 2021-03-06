# -*- coding: utf-8 -*-
"""
Created on Mon Oct 19 17:07:56 2015

@author: warriorzhai
"""

import datetime
import settings
import pandas as pd
from dateutil.relativedelta import relativedelta
import csv
import sys
import os
from os import listdir
from os.path import isfile, join

date_format=settings.date_format
current_path=os.getcwd()+'\\'
defaultna=settings.defaultna


def test():
    base=Base()
    #date_quater(self,date):
    base.date_quater()
    base.date_quater('2016-01-01')
    #date_yqmd(self,date)
    base.date_yqmd()
    base.date_yqmd('2016-01-01')
    #str_to_datetime(self,s,dateformat=date_format)
    base.str_to_datetime('2016-01-01')
    base.str_to_datetime(['2016-01-01','2016-02-01'])
    # date_togap(self,date,dateformat=date_format,gap_type=0,gap_val=0)
    base.date_togap(gap_type=0,gap_val=10)
    base.date_togap(gap_type=0,gap_val=-10)
    base.date_togap(gap_type=1,gap_val=1)
    base.date_togap(gap_type=2,gap_val=1)
    base.date_togap('2016-01-01',gap_type=1,gap_val=1)
    

class Base(object):
    
        def __init__(self):
            pass
    #==============装饰器==================================
        def deco_dateformat(func):
            def _defaultdate(self,date=defaultna,dateformat=date_format,
                             *args,**kargs):
                #输入date默认值
                if date==defaultna:
                    date=datetime.datetime.now()
                #输入str转为datetime
                if type(date)==str:
                    date=self.str_to_datetime(date,dateformat)
                
                return func(self,date,*args,**kargs)
            return _defaultdate
        
        def deco_iterresult(func):
            def _iterresult(self,isiterx,*args,**kargs):
                if self.is_iter(isiterx):
                    return [func(self,x) for x in isiterx]
                else:
                    return func(self,isiterx)
            return _iterresult
        
    #=====================日期处理================================
        #date---datetime
        @deco_dateformat
        def date_quater(self,date):
            return date.month/3+int((date.month%3)>0)
        
        #date---str
        @deco_dateformat
        def date_yqmd(self,date):
            vals=[date.year,self.date_quater(date),date.month,date.day]
            return vals
            
            

            #获取当日日期的str格式
            #导入：datetime
            #返回：当日字符串
            #参数说明：
            #date_format--日期的输出格式，字符串
            #date--参考日期，默认为当天, 格式是%Y-%m-%d
            #gap--输出参考日期的日期间隔
            #gap_type---日期间隔的类别，0day1month2year
        @deco_dateformat
        def date_togap(self,date,dateformat=date_format,gap_type=0,gap_val=0):
            
            gap_types=['days','months','years']
            #转化为string格式
            tar_date=date + relativedelta(**{gap_types[gap_type]:gap_val})
            return self.datetime_to_str(tar_date,dateformat)
        
        def date_today(self):
            return self.date_togap()
            
        #将str list 或str转为datetime
        @deco_iterresult
        def str_to_datetime(self,s,dateformat=date_format):
            return datetime.datetime.strptime(s,dateformat)

        
        #将str list 或str转为datetime
        @deco_iterresult
        def datetime_to_str(self,s,dateformat=date_format):
            return s.strftime(dateformat)
                
        
        def gap_to_today(self,date,loc_format='%Y-%m-%d',gatype=1):
            gap_div=[1.0,30.0,360.0]
            if not isinstance(date, datetime.date):
                try:
                    date=datetime.datetime.strptime(date,loc_format)
                except ValueError:
                    return None
            today=datetime.datetime.strptime(self.today_as_str(),date_format)
            gap=today-date
            return round(gap.days/gap_div[gatype])

        def to_lower(self,strs):
            return map(str.lower,strs)
        
        def lists_minus(self,lleft,lright):
            return [l for l in lleft if l not in lright]
        
        #相加后没有重复项
        #l2可以是非iter
        def lists_add(self,l1,l2):
            l2=l2 if self.is_iter(l2) else [l2]
            return list(set(l1+l2))
            
        def any_2list(self,obj):
            return obj if self.is_iter(obj) else [obj] 
        
        #判断是不是可迭代的
        #list、tuple或迭代器返回True
        #string、dict的返回结果都是False
        def is_iter(self,x):
            if hasattr(x, '__iter__') and type(x)!=dict:
                return True
            else:
                return False
        
        #一组datetime或者strdate中最新的一天
        #返回strdate
        def date_newest(self,datelist):
            if not self.is_iter(datelist):
                return datelist
            
            if type(datelist[0]==str):
                datelist=self.str_to_datetime(datelist)
            
            return self.datetime_to_str(max(datelist))
        
        def date_minus(self,date1,date2):
            date1,date2=[self.str_to_datetime(date) if type(date)==str else date for date in [date1,date2]]
            return (date1-date2).days
        
        def date_inc(self,date):
            return self.today_as_str(base_date=date,gap_val=1)
            
        def date_sort(self,dates,dateformat=date_format):
            pass
            
        
            #将一个字典列表中，所有指定key的值导出
            #为防万一，使用如果key不存在于某个字段会报错
            #参数说明：
            #key--指定要查找的key值
            #diclist--一个dict的列表，如[{dic1},{dic2}]
            #返回：
            #dict[key]的列表
        def unpack_dic(self,key,diclist):
                #为防万一，使用如果key不存在于某个字段会报错
                try:
                    res=[i[key] for i in diclist]
                except KeyError:
                    print '字典中key值不存在'
                    sys.exit()

                return res
        
        
        def addkey_2dictlist(self,dictlist,key,val):
            addvallist=val if self.is_iter(val) else [val]*len(dictlist)
            return map(lambda d,v:dict(d,**{key:v}),dictlist,addvallist)
            
        #input--['k1','k2'],[1,2]
        #output--[{'k1': 1}, {'k2': 2}]
        def lists_2dictlists(self,key_list,val_list):
            f=lambda k,v:self.lists_2dict(k,v)
            return map(f,key_list,val_list)
        
        #input--['k1','k2'],[1,2]
        #output--{'k1': 1, 'k2': 2}
        def lists_2dict(self,key_list,val_list):
            if not self.is_iter(key_list):
                return {key_list:val_list}
            val_list=self.any_2list(val_list)
            return dict(zip(key_list,val_list))
        
        
        #将一个字典列表中，合并相同key值的value为list
        #例子：
        #将字典diclist=[{'k1':1,'k2':2},{'k1':3,'k2':4}]合并
        # merge_dic_values(diclist)---》{'k1':[1,2],'k2':[3,4]}
            #参数说明：
            #it---可以iterate的一串字典
            #返回：
            #合并后的字典
        def merge_dic_values(self,it):
            keys=it[0].keys()
            new_dic={}
            for k in keys:
                 new_dic[k]=self.unpack_dic(k,it)
            return new_dic
    
        
        #将pandas group转为字典格式
        def pdgrp_to_dic(self,pdgrp):
            pdgrp_dic={}
            for na,val in pdgrp:
                print '按类别获取股票列表......'
                try:
                    pdgrp_dic[na]=list(set(val.tolist()))
                except:
                    pdgrp_dic[na]=val
                    continue
            return pdgrp_dic
            
        
        #中文编码，转为utf-8
        def zhs_decode(self,zhs):
            if not hasattr(zhs, '__iter__'):
                return unicode(zhs,encoding = "utf-8")
            else:
                return [unicode(zh,encoding = "utf-8") for zh in zhs]    
        
        #将str转为整数
        #s--str
        #如果输入不能转为int则返回False
        def type_to_type(self,data,totype):
            data=self.any_2list(data)
            res=[]
            for x in data:
                try:
                    res=res+[totype(x)]
                except ValueError:
                    print '无法转换类型'
                    return False
            return res
        
        def int_to_strdate(self,intdate):
            intdate=self.any_2list(intdate)
            res=[]
            for x in intdate:
                strdate=str(x)
                res=res+[strdate[:4]+'-'+strdate[4:6]+'-'+ strdate[6:8]]
            return res
        
        def strdate_calc(self,strdate,gap=1):
            date=self.str_to_datetime(strdate)
            return self.datetime_to_str(date+datetime.timedelta(days=gap))
        

        
        #数值转为位数为l的string，不足长度的部分用pad填充     
        #front--在前方补零，back--在后方补零
        #pad--填充的数字，str
        #返回：
        #str
        def int_to_str(self,i,l=6,pad='0',direct='front'):
            to_str=str(i)
            if direct=='front':
                return pad*(l-len(to_str))+to_str
            else:
                return to_str+pad*(l-len(to_str))
        
        def strlist_build(self,s,varlist):
            print varlist
            f=lambda x: tuple(x) if self.is_iter(x) else (x,)
            return [s % f(var) for var in varlist]
        
        #将list of string连接为逗号分隔的string格式
        #参数说明：
        #li--list of string/string
        #例子：
        #li=['1','2','3']
        #list_to_str(li)--->'1,2,3'
        #li=['1']
        #list_to_str(li)--->'1'
        #li=''
        #list_to_str(li)--->''
        #li=[]
        #list_to_str(li)--->''
        #返回
        #逗号分隔的string,或 ''
        def list_to_str(self,li):
            if type(li)==list or type(li)==tuple:
                li=','.join(li)
            return li
        
        def get_val_rank(self,val,sample):
            return round((sample<=val).mean(),2)*100

        
        #将pandas的df的列转为字典返回
        #例子：df=
        #col1 col2
        #a1   b1
        #a2   b2
        #将df的每一列，列名作为key，列的内容转为list value，构成一个字典返回
        #pd_df2dic(self,df,df.columns)
        def pd_df2dic(self,df,keys):
            vals=[df[n].tolist() for n in df.columns]
            return dict(zip(keys, vals)) 
        
        #将pandas的df的每行转为字典列表返回
        def pd_df2diclist(self,df,index=False):
            if index:
                df.reset_index(level=0, inplace=True)
            return df.T.to_dict().values()
        
        #keys=[key1,key2]
        #val_lists=[[vallist1],[vallist2]]
        def lists_todiclist(self,keys,val_lists):
            vals=zip(*val_lists)
            return [dict(zip(keys,val)) for val in vals]
        
        def sort_diclist_byval(self,diclist,sort_key):
            return sorted(diclist, key=lambda k: k[sort_key]) 
        
        #一个df列表，以列df_init为标准，merge列表中所有df，返回merge结果
        #df_list---iter of df,待合并的df列表
        #df_init--list/pd.series 合并的参照列
        #merge_on--每个df中合并参照的列的名称
        def merge_format(self,df_init,df_list,merge_on,df_names=[],merge_mode='outer'):
            merged_df=pd.DataFrame()
            merged_df[merge_on]=df_init
            #检查df_list必须是可iter 的
            if not hasattr(df_list, '__iter__'):
                df_list=[df_list]
            if len(df_names)>0:
                for df,na in zip(df_list,df_names):
                    if not df is None:
                        print na
                        df.columns=[merge_on]+[na]*(len(df.columns)-1)
                        merged_df=pd.merge(merged_df,df,on=merge_on, how=merge_mode)
            else:
                print 'perform'
                for df in df_list:
                    if not df is None:
                        merged_df=pd.merge(merged_df,df,on=merge_on, how=merge_mode)

            return merged_df
            
        
        #将dataframe写入当前工作路径的csv
        #参数说明
        #df--数据
        #filename--文件名
        def write_df_csv(self,df,file_name,date=False):     
            if date:
                today=self.today_as_str()
                #文件名格式为
                #指定file_name_今日日期.csv
                file_name=file_name+'_'+today
            df.to_csv(file_name+'.csv',encoding='utf-8')
            
        def read_csv(self,name,route='',header=0,index_col=0):
            df=pd.read_csv(route+name,header=header,index_col=index_col)
            return df
        
        def read_csvs(self,route):
            abs_route=current_path+route
            if not abs_route.endswith('\\'):
                abs_route=os.path.join(abs_route, '')
            file_names = [f for f in listdir(abs_route) if isfile(join(abs_route, f)) \
            and f.endswith('.csv')]
            for na in file_names:
                yield (na,self.read_csv(na,abs_route))
        
        def write_dic_csv(self,filename,dic,header=[],header_direct=0):
            fp=open(filename+'.csv', 'w')
            wt = csv.writer(fp, delimiter=',')
            if header_direct:
                for h,dts in zip(header,dic.values()):  
                    wt.writerow([h]+dts)
            else:
                wt.writerow(header)
                for dts in dic.values():  
                    wt.writerow(dts)
            fp.close()

        def write_dics_csv(self,filename,data,header=[]):
            fp=open(filename+'.csv', 'w')
            wt = csv.writer(fp, delimiter=',')
            wt.writerow(header)
            for dts in data:  
                wt.writerow(dts.values())
            fp.close()
    
    