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

class Base(object):
    
        def __init__(self):
            pass

            #获取当日日期的str格式
            #导入：datetime
            #返回：当日字符串
            #参数说明：
            #date_format--日期的输出格式，字符串
            #date--参考日期，默认为当天, 格式是%Y-%m-%d
            #gap--输出参考日期的日期间隔
            #gap_type---日期间隔的类别，0day1month2year
        def today_as_str(self,base_date='',dateformat=date_format,gap_type=0,gap_val=0):
            
            gap_types=['days','months','years']
            gap_arg={gap_types[gap_type]:gap_val}
            
            if not base_date:
                #获取当前日期datetime.datetime格式
                base_date = datetime.datetime.now()
            else:
                base_date=datetime.datetime.strptime(base_date,dateformat)
            
            #转化为string格式
            tar_date=base_date + relativedelta(**gap_arg)
            
            return tar_date.strftime(dateformat)
        
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
        
        def is_iter(self,x):
            if hasattr(x, '__iter__'):
                return True
            else:
                return False
            
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
        
        
        def listpair_2dict(self,key_list,val_list):
            return [{x[0]:x[1]} for x in zip(key_list,val_list)]
        
        
        
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
        def string_to_int(self,s):
            try:
                i=int(s)
                return i
            except ValueError:
                print '无法将字符转为数值'
                return False
        
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
        def pd_df2diclist(self,df):
            return df.T.to_dict().values()
        
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
    
    