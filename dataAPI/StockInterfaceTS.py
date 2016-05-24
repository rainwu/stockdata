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

class StockInterfaceTS(object):
    
    def __init__(self,token=settings.token):
        self.token=settings.token
        self.ba=Base()
    

        
    #ts接口数据抓取执行程序
    #参数说明：
    #itf：接口函数名称或函数变量，如fd.FundDiv
    #itf_paras:接口参数,字典
    #返回：
    #pandas dataframe
    def _getdata(self,itf,itf_paras={}):
        #请求获取数据
        #如果连接失败：
        #休息sleep_time秒，重复请求settings.try_times次
#        for i in range(settings.try_times):
#            try:
#                res=itf(**itf_paras)
#            except KeyboardInterrupt:
#                print '连接出错，第'+str(i)+'重试中......'
#                time.sleep(settings.sleep_time)
#                continue
#            break
##        
        res=itf(**itf_paras)
        return res
    
#=================基本信息===========================
    #股票概念分类
    #来自sina财经
    #返回:
    #所有股票的概念分类表 pandas dataframe
    def getConCla(self):
        #定义接口，定义接口参数
        itf=ts.get_concept_classified
        #获取数据
        res=self._getdata(itf)
        return res
    
    #股票行业分类TS版本
    #返回：
    #所有股票的行业分类表， pandas dataframe
    def getIndCla(self):
        #定义接口，定义接口参数
        itf=ts.get_industry_classified
        #获取数据
        res=self._getdata(itf)
        return res
    
    #股票行业分类通联版本
    def getEquInd(self,ind_code='010303'):
        #定义接口，定义接口参数
        instance=ts.Equity()
        itf=instance.EquIndustry
        itf_paras={'industryVersionCD':ind_code}
        #获取数据
        res=self._getdata(itf,itf_paras)
        return res
    
    #股票基本数据通联版本
    def getEqu(self,ticker='',equTypeCD='A'):
        #定义接口，定义接口参数
        instance=ts.Equity()
        itf=instance.Equ
        itf_paras={'equTypeCD':equTypeCD,'ticker':ticker}
        #获取数据
        res=self._getdata(itf,itf_paras)
        return res
        
    
    
    #股票基本面数据TS版本
    #返回:
    #所有股票的概念分类表 pandas dataframe
    def getStoBas(self):
        itf=ts.get_stock_basics
        res=self._getdata(itf)
        return res
    
    #获取创业板股票列表
    def getGemCla(self):
        itf=ts.get_gem_classified
        res=self._getdata(itf)
        return res
        
    #获取沪深300股票列表和权重  
    def getHs300(self):
        itf=ts.get_hs300s
        res=self._getdata(itf)
        return res
        
    
    
    #指数基本数据
    #参数说明
    #ticker--指数代码，str或list of str
    #field--指定返回的项，list或''
    #返回;
    #dataframe
    def getIndex(self,ticker='',field='',secID=''):
        #输入处理
        #将ticker list转为满足通联接口的str格式
        ticker=self.list_to_str(ticker)
        #定义接口，定义接口参数
        instance=ts.Idx()
        itf=instance.Idx
        itf_paras={'ticker':ticker,'field':field,
                   'secID':secID}
        res=self._getdata(itf,itf_paras)
        return res
        
    
    #指数成分信息
    #参数说明
    #ticker--指数代码，str或list of str
    #field--指定返回的项，list或''
    #返回;
    #dataframe
    def getIndexCons(self,ticker='',field=''):
        #输入处理
        #将ticker list转为满足通联接口的str格式
        ticker=self.list_to_str(ticker)
        #取某一天的指数成分表，默认为当日
#        if len(intoDate)==0:
#            intoDate=self.ba.today_as_str(date_format="%Y-%m-%d")
        #定义接口，定义接口参数
        instance=ts.Idx()
        itf=instance.IdxCons
        itf_paras={'ticker':ticker,'field':field}
        res=self._getdata(itf,itf_paras)
        return res
    
    #H表示停牌，R表示复牌
    def getSecTips(self,tipsTypeCD='H'):
        instance=ts.Market()
        itf=instance.SecTips
        itf_paras={'tipsTypeCD':tipsTypeCD}
        res=self._getdata(itf,itf_paras)
        return res
    
#=============事件信息===================================
    #全部新股信息
    #返回:
    #所pandas dataframe
    def getNewSto(self):
        itf=ts.new_stocks
        res=self._getdata(itf)
        return res
    
    ##基金持股数据
    def getFudHod(self,year='',quater=''):
        
        now = datetime.datetime.today()
        if not year or year>now.year:
            year=now.year
        if not quater:
            if year<now.year:
                quater=4
            else:
                quater=(now.month-1)/3
                if quater==0:
                    quater=4
                    year=year-1
        
        itf=ts.fund_holdings
        itf_paras={'year':year,'quater':quater}
        res=self._getdata(itf,itf_paras)
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
    def getHDat(self,code,start='',end='',index=False):
        
        #输入处理
        #默认值处理
        if len(end)==0:
            end=self.ba.today_as_str()
            
        #定义接口，定义接口参数
        itf=ts.get_h_data
        itf_paras={'code':code,'start':start,'end':end,'index':index}
        
        #获取数据
        res=self._getdata(itf,itf_paras)
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
    def getHisDat(self,code,start='',end='',ktype='5'):
        
        #输入处理
        #默认值处理
        if len(start)==0:
            start=self.ba.today_as_str()
        if len(end)==0:
            end=self.ba.strdate_calc(start,gap=1)
        #定义接口，定义接口参数
        itf=ts.get_hist_data
        itf_paras={'code':code,'start':start,'end':end,'ktype':ktype}
        print itf_paras
        #获取数据
        res=self._getdata(itf,itf_paras)
        return res
    
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
        print ticker
        itf_paras={'ticker':ticker,'field':field,
                   'secID':secID,'intoDate':intoDate}
        res=self._getdata(itf,itf_paras)
        return res

        

    
    
    
    
    
    
    
    
    
    
    
    
    