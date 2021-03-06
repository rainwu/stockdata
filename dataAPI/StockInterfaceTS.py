# -*- coding: utf-8 -*-
"""
Created on Thu Nov 12 15:32:21 2015

@author: saisn
"""
#股票接口程序修改
#使用ts提供的通联数据的接口
#接口类用于从接口获取信息
#数据处理类对接口类获取的数据（df）进行处理，如过滤等
#一种选择，将处理的数据写入csv，可以用r进行分析处理
#一种选择，将处理的数据写入数据库

#StockBase:一些常用的基本函数

import tushare as ts
import time
import datetime
import os
try:
    os.chdir(os.environ["WORK_PATH"])
except KeyError:
    pass
import settings
from Base import Base

date_format=settings.date_format
emptylist=settings.emptylist
defaultna=settings.defaultna

class DataCapture(object):
    
    def __init__(self,token=settings.token):
        self.token=settings.token
        self.base=Base()
    
    def isEmptydf(self,df):
        return df is None or min(df.shape)==0
    
        #数据抓取,实现网络问题时重新抓取
    #参数说明：
    #request_itf[function]：request的接口函数，如fd.FundDiv
    #*args, **kwargs:request的接口函数参数
    #返回：
    #视接口函数返回而定，tushare和通联的返回pd.dataframe
    #**如果返回值为空，则也返回
    def data_request(self,request_itf,*args, **kwargs):
        
        for i in range(settings.try_times):
            try:
                res=request_itf(*args, **kwargs)
            except KeyboardInterrupt:
                print '连接出错，第'+str(i)+'重试中......'
                time.sleep(settings.sleep_time)
                continue
            break
        if res is None:
            pass#LOG
        return res

    
    #抽取数据中的行、列  考虑使用PandaSQL
    #df--pd.dataframe,带抽取的数据框
    #select_colnams--list of str or str,df的列名
    #select_rows--dict, {'colnam':[rowvalues],'colnam':[rowvalues]...}
    #返回：
    #如果select_xxx是列表，则返回pd.df；如果是str，则返回pd.series
    def data_extract(self,df,select_colnams=defaultna,select_rows=defaultna):
        
        #抽取列
        #列名不存在，返回non
        def data_extractcol(df,select_colnams):
            if select_colnams in emptylist:
                return df
            else:
                try:
                    return df[select_colnams]
                except KeyError:
                    pass#LOG
                    return defaultna
        
        #按照index值抽取行
        def _data_extractrow_byindex(df,index_vals):
            if index_vals in emptylist:
                return df
            try:
                return df.loc[df.index.isin(index_vals)]
            except KeyError:
                pass#LOG
                return defaultna
        
        #按行的值抽取行
        def _data_extractrow_byval(df,col_nam,row_vals):
            if row_vals in emptylist:
                return df
                
            row_vals=self.base.any_2list(row_vals)
            
            try:
                return df.loc[df[col_nam].isin(row_vals)]
            except KeyError:
                pass#LOG
                return defaultna 
        
        #行名不存在返回空df
        def data_extractrow(df,select_rows):
            if select_rows in emptylist:
                return df
            
            indexnam=df.index.name
            
            for colnam,rowval in select_rows.items():
                df=_data_extractrow_byindex(df,rowval) if colnam==indexnam \
                    else _data_extractrow_byval(df,colnam,rowval)
            return df
            
        df_rowextracted=data_extractrow(df,select_rows)
        
        return df_rowextracted if self.isEmptydf(df_rowextracted) \
            else data_extractcol(df_rowextracted,select_colnams)

    
    #所有接口通用的数据抓取流程    
        
    def data_capture_flow(self,request_itf,select_colnams=defaultna,
                      select_rows=defaultna,*args,**kwargs):
                          
        datadf=self.data_request(request_itf,*args, **kwargs)
        datadf_extracted=self.data_extract(datadf,select_colnams,select_rows)
        return datadf_extracted
        
def test_DataCapture():
    cap=DataCapture()
    #data_request(self,request_itf,*args, **kwargs)
    request_itf=ts.get_concept_classified
    data=cap.data_request(request_itf)
    print data.head()
    #data_extractcol(df,select_colnams):
    import pandas as pd
    df=pd.DataFrame({'col1':[1,2,3],'col2':[4,5,6]},index=['i1','i2','i3'])
    select_colnams_instances=[['col1'],'col1',None,[],['col1','col2'],
                              'xxx',['col1','xxx']]
    data=iter([data_extractcol(df,x) for x in select_colnams_instances])
    next(data)
    #_data_extractrow_byindex(df,index_vals)
    df=pd.DataFrame({'col1':[1,2,3],'col2':[4,5,6]},index=['i1','i2','i3'])
    index_vals_instances=[None,[],'',['i1','i2'],'i1',['i4','i1'],['i4'],'i4']
    data=iter([_data_extractrow_byindex(df,x) for x in index_vals_instances])
    next(data)
    #_data_extractrow_byval(df,col_nam,row_vals):
    df=pd.DataFrame({'col1':[1,2,3],'col2':[4,5,6]},index=['i1','i2','i3'])
    col_nam='col2'
    row_vals_instances=[None,[],'',1,[1],4,[4,6],[4,7]]
    data=iter([_data_extractrow_byval(df,col_nam,x) for x in row_vals_instances])
    next(data)
   
def test_StockInterfaceTS():
    ex=StockInterfaceTS()
    ticker=['000001',['000001'],'100000']
    data=iter([ex.getEqu(x) for x in ticker])
    next(data)
    # getFudHod(self,year=defaultna,quater=4,select_colnams=defaultna,select_rows=defaultna)
    res=ex.getFudHod(2015,1,select_colnams='code')     
    res=ex.getFudHod(2015,1,select_colnams=['code','nums']) 
    res=ex.getFudHod(2015,1,select_colnams='notexit')
    res=ex.getFudHod(2015,1,select_colnams=['code','notexit'])
    res=ex.getFudHod(2015,1,select_rows={'code':'603993','nums':15})  
    res=ex.getFudHod(2015,1,select_rows={'code':['603993','000001']},select_colnams='nums')
    res=ex.getFudHod(2015,1,select_rows={'code':[]},select_colnams='nums')  
    res=ex.getFudHod(2015,1,select_rows={'code':'1000001'},select_colnams='nums') 
    res=ex.getFudHod(2015,1,select_rows={'code':['603993','1000001']})
    res=ex.getFudHod(2015,1,select_rows={'notexit':['603993','000001']},select_colnams='nums')
    res=ex.getFudHod(2015)    
    res=ex.getFudHod(2017)
    res=ex.getFudHod(2017,3)
    res=ex.getFudHod()
    res=ex.getFudHod(2016,1)
    res=ex.getFudHod(2016)
    res=ex.getFudHod(2016,2)
    res=ex.getFudHod(2015,9)
    #getHDat(self,code,start,end,index=False,select_colnams=defaultna,select_rows=defaultna)
    code='000001'
    res=ex.getHDat(code=code)
    res=ex.getHDat(code=code,start='2016-06-20')
    res=ex.getHDat(code=code,start='2016-06-20',end='2016-06-20')
    res=ex.getHDat(code=code,start='2016-06-11',end='2016-06-12')
    res=ex.getHDat(code='000002')
    res=ex.getHDat(code='0000021')
    
    
#tushare接口
class StockInterfaceTS(object):
    
    def __init__(self,token=settings.token):
        self.token=settings.token
        self.base=Base()
        self.cap=DataCapture()
    
    #==============装饰器==================================
    def deco_defaultdate(func):
        def _defaultdate(self,start=defaultna,end=defaultna,*args,**kargs):
                #输入date默认值
            if end==defaultna:
                end=self.base.date_today()
            if start==defaultna:
                start=self.base.date_togap(end,gap_type=1,gap_val=-1)
                
            return func(self,start=start,end=end,*args,**kargs)
            
        return _defaultdate
    
    
#=================基本信息==========================
    
    #股票概念分类
    #来自sina财经
    #返回:
    #所有股票的概念分类表 pandas dataframe
    def getConCla(self,select_colnams=defaultna,select_rows=defaultna):
        #定义接口，定义接口参数
        request_itf=ts.get_concept_classified
        #获取数据
        res=self.cap.data_capture_flow(request_itf,select_colnams,
                      select_rows)
        return res
    
    #股票行业分类TS版本
    #返回：
    #所有股票的行业分类表， pandas dataframe
    def getIndCla(self,select_colnams=defaultna,select_rows=defaultna):
        #定义接口，定义接口参数
        request_itf=ts.get_industry_classified
        #获取数据
        res=self.cap.data_capture_flow(request_itf,select_colnams,
                      select_rows)
        return res
    
    #股票行业分类通联版本
    def getEquInd(self,ind_code='010303',select_colnams=defaultna,
                  select_rows=defaultna):
        #定义接口，定义接口参数
        instance=ts.Equity()
        request_itf=instance.EquIndustry
        #获取数据
        res=self.cap.data_capture_flow(request_itf,select_colnams,
                      select_rows,industryVersionCD=ind_code)
        return res
    
    #股票基本数据通联版本
    def getEqu(self,ticker,equTypeCD='A',select_colnams=defaultna,
                  select_rows=defaultna):
        #定义接口，定义接口参数
        instance=ts.Equity()
        request_itf=instance.Equ
        #获取数据
        res=self.cap.data_capture_flow(request_itf,select_colnams,
                      select_rows,equTypeCD=equTypeCD,ticker=ticker)
        return res
        
    
    
    #股票基本面数据TS版本
    #返回:
    #所有股票的概念分类表 pandas dataframe
    def getStoBas(self,select_colnams=defaultna,
                  select_rows=defaultna):
        request_itf=ts.get_stock_basics
        res=self.cap.data_capture_flow(request_itf,select_colnams,
                      select_rows)
        return res
    
    #获取创业板股票列表
    def getGemCla(self,select_colnams=defaultna,
                  select_rows=defaultna):
        request_itf=ts.get_gem_classified
        res=self.cap.data_capture_flow(request_itf,select_colnams,
                      select_rows)
        return res
        
    #获取沪深300股票列表和权重  
    def getHs300(self,select_colnams=defaultna,
                  select_rows=defaultna):
        request_itf=ts.get_hs300s
        res=self.cap.data_capture_flow(request_itf,select_colnams,
                      select_rows)
        return res
        
    
    
#    #指数基本数据
#    #参数说明
#    #ticker--指数代码，str或list of str
#    #field--指定返回的项，list或''
#    #返回;
#    #dataframe
#    def getIndex(self,ticker='',field='',secID=''):
#        #输入处理
#        #将ticker list转为满足通联接口的str格式
#        ticker=self.list_to_str(ticker)
#        #定义接口，定义接口参数
#        instance=ts.Idx()
#        itf=instance.Idx
#        itf_paras={'ticker':ticker,'field':field,
#                   'secID':secID}
#        res=self._getdata(itf,itf_paras)
#        return res
#        
#    
#    #指数成分信息
#    #参数说明
#    #ticker--指数代码，str或list of str
#    #field--指定返回的项，list或''
#    #返回;
#    #dataframe
#    def getIndexCons(self,ticker='',field=''):
#        #输入处理
#        #将ticker list转为满足通联接口的str格式
#        ticker=self.list_to_str(ticker)
#        #取某一天的指数成分表，默认为当日
##        if len(intoDate)==0:
##            intoDate=self.ba.today_as_str(date_format="%Y-%m-%d")
#        #定义接口，定义接口参数
#        instance=ts.Idx()
#        itf=instance.IdxCons
#        itf_paras={'ticker':ticker,'field':field}
#        res=self._getdata(itf,itf_paras)
#        return res
#    
#    #H表示停牌，R表示复牌
#    def getSecTips(self,tipsTypeCD='H'):
#        instance=ts.Market()
#        itf=instance.SecTips
#        itf_paras={'tipsTypeCD':tipsTypeCD}
#        res=self._getdata(itf,itf_paras)
#        return res
    
#=============事件信息===================================
    #全部新股信息
    #返回:
    #所pandas dataframe
    def getNewSto(self,select_colnams=defaultna,
                  select_rows=defaultna):
        request_itf=ts.new_stocks
        res=self.cap.data_capture_flow(request_itf,select_colnams,
                      select_rows)
        return res
    
    ##基金持股数据,输入的日期大于最新一期数据日期，则返回最新一期数据
    #year--数据年份
    def getFudHod(self,year=defaultna,quarter=4,select_colnams=defaultna,
                  select_rows=defaultna):
        
        #获取当前数据库中最新季报
        #从当前日期的前三个月算出上个季度的，年，季
        y,q,m,d=self.base.date_yqmd( \
            self.base.date_togap(gap_type=1,gap_val=-3))
        
        #如果输入年份超限或不存在，返回默认最近一期的季报
        #输入年份为当年，季度取输入值和最近一期季度的最小
        if year==defaultna or year>y:
            print '年份超限或未赋值，获取最近一期数据...'
            year,quarter=y,q
        elif year==y:
            quarter=min(quarter,q)
        
        request_itf=ts.fund_holdings
        res=self.cap.data_capture_flow(request_itf,select_colnams,
                      select_rows,year=year,quarter=quarter)
        return res

#============交易数据====================================
    #某只股票/指数在一段时间范围内的复权交易数据，日线
    #参数说明：
    #code：股票代码，必输，string
    #start:开始日期 ,string,format：YYYY-MM-DD 为空时取去年今日
    #end:string,结束日期 format：YYYY-MM-DD 为空时取当前日期
    #index：False为股票，True为基金
    #返回:
    #pandas dataframe
    #停牌、不存在的股票返回none
    #非交易日返回none
    @deco_defaultdate
    def getHDat(self,code,start=defaultna,end=defaultna,index=False,select_colnams=defaultna,
                  select_rows=defaultna):
        #定义接口，定义接口参数
        request_itf=ts.get_h_data
        #获取数据
        res=self.cap.data_capture_flow(request_itf,select_colnams,
                      select_rows,code=code,start=start,end=end,index=index)
        return res
    
    #某只股票或指数（几个主要的）在某天到某天的交易数据，5分钟 15=15分钟 30=30分钟 60=60分钟线
    #只限于近三年范围内的某天
    #code中基金类型：
    #（sh=上证指数 sz=深圳成指 hs300=沪深300指数 sz50=上证50 zxb=中小板 cyb=创业板）
    #参数说明：
    #code：股票代码，必输，string
    #start:开始日期 ,string,format：YYYY-MM-DD 为空时取去年今日
    #end:string,结束日期 format：YYYY-MM-DD 为空时取当前日期
    #index：False为股票，True为基金
    #返回:
    #pandas dataframe
    @deco_defaultdate
    def getHisDat(self,code,start=defaultna,end=defaultna,ktype='5',select_colnams=defaultna,
                  select_rows=defaultna):
        #定义接口，定义接口参数
        request_itf=ts.get_hist_data
        #获取数据
        res=self.cap.data_capture_flow(request_itf,select_colnams,
                      select_rows,code=code,start=start,end=end,ktype=ktype)
        return res
        
#==================修改完毕分界线========================
    
    #某只股票在某天的成交逐笔
    #参数说明：
    #code：股票代码，必输，string
    #date：日期，格式YYYY-MM-DD
    #返回:
    #pandas dataframe
    def getTikDat(self,code,date=''):
        
        #输入处理
        #默认值处理
        if len(date)==0:
            date=self.ba.today_as_str()
        #定义接口，定义接口参数
        itf=ts.get_tick_data
        itf_paras={'code':code,'date':date}
        #获取数据
        res=self._getdata(itf,itf_paras)
        return res
        
        
    #沪市融资融券统计信息
    def getShMar(self,start,end=''):
        #输入处理
        #默认值处理
        if len(end)==0:
            end=self.ba.today_as_str()
            
        itf=ts.sh_margins
        itf_paras={'start':start,'end':end}
        #获取数据
        res=self._getdata(itf,itf_paras)
        return res
    
    #深市融资融券统计信息
    def getSzMar(self,start,end=''):
        #输入处理
        #默认值处理
        if len(end)==0:
            end=self.ba.today_as_str()
            
        itf=ts.sz_margins
        itf_paras={'start':start,'end':end}
        #获取数据
        res=self._getdata(itf,itf_paras)
        return res
    
    #沪市融资融券,单只股票明细
    def getShMarDetS(self,symbol,start,end=''):
        #输入处理
        #默认值处理
        if len(end)==0:
            end=self.ba.today_as_str()
            
        itf=ts.sh_margins
        itf_paras={'symbol':symbol,'start':start,'end':end}
        #获取数据
        res=self._getdata(itf,itf_paras)
        return res
    
    #沪市融资融券,所有股票当天明细
    def getShMarDetA(self,date=''):
        #输入处理
        #默认值处理
        if len(date)==0:
            date=self.ba.today_as_str()
            
        itf=ts.sh_margin_details
        itf_paras={'date':date}
        #获取数据
        res=self._getdata(itf,itf_paras)
        return res
    
    #深市融资融券,所有股票当天明细
    def getSzMarDetA(self,date=''):
        #输入处理
        #默认值处理
        if len(date)==0:
            date=self.ba.today_as_str()
            
        itf=ts.sz_margin_details
        itf_paras={'date':date}
        #获取数据
        res=self._getdata(itf,itf_paras)
        return res
        
    #所有股票当前的交易信息
    def getTodAll(self):
        #定义接口，定义接口参数
        itf=ts.get_today_all
        #获取数据
        res=self._getdata(itf)
        return res    
    
    #主要指数当前的交易信息
    def getIndTra(self):
        #定义接口，定义接口参数
        itf=ts.get_index
        #获取数据
        res=self._getdata(itf)
        return res    
    
    def getMktEqu(self,ticker='',field='',beginDate='',endDate='',tradeDate=''):
        instance=ts.Market()
        itf=instance.MktEqud
        ticker=self.list_to_str(ticker)
        itf_paras={'ticker':ticker,'field':field,'beginDate':beginDate,'endDate':endDate,
                   'tradeDate':tradeDate}
        res=self._getdata(itf,itf_paras)
        return res
    

    #基金信息
    def getFund(self,ticker='',field='',etfLof='',listStatusCd='L',secID=''):
        instance=ts.Fund()
        itf=instance.Fund
        ticker=self.list_to_str(ticker)
        itf_paras={'ticker':ticker,'field':field,'etfLof':etfLof,'listStatusCd':listStatusCd,
                   'secID':secID}
        res=self._getdata(itf,itf_paras)
        return res
    

    
    def getIdxCons(self,ticker='',field='',secID='',intoDate=''):
        instance=ts.Idx()
        itf=instance.IdxCons
        ticker=self.list_to_str(ticker)
        itf_paras={'ticker':ticker,'field':field,
                   'secID':secID,'intoDate':intoDate}
        res=self._getdata(itf,itf_paras)
        return res

        

    
    
    
    
    
    
    
    
    
    
    
    
    