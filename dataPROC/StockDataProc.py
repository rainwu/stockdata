# -*- coding: utf-8 -*-
"""
Created on Tue Feb 02 16:15:35 2016

@author: warriorzhai
"""

#常用股票数据，获取函数

import os
try:
    os.chdir(os.environ["WORK_PATH"])
except KeyError:
    pass
import settings
import pandas as pd
from Base import Base 
from dataAPI.StockInterfaceWrap import StockInterfaceWrap
from dataPROC.StockDataStat import StockDataStat
from databaseAPI.DatabaseInterface import DatabaseInterface
from databaseAPI.DatabaseProc import DatabaseProc

date_format=settings.date_format

class StockDataProc(object):
    
    def __init__(self,token=settings.token):
        self.token=settings.token
        self.base=Base()
        self.wp=StockInterfaceWrap()
        self.stat=StockDataStat()
        self.db_proc=DatabaseProc()
        
    #给起始和终止日期，返回期间的行
    #colnam是date类型所在的列
    def _sel_row_bydate(self,df,start,end=''):
        #将输入日期转为datetime
        if not end:
            end=self.base.today_as_str()
        start_dt=self.base.str_to_datetime(start)
        end_dt=self.base.str_to_datetime(end)
        
        date_col=pd.to_datetime(df.index)  
        mask = (date_col>= start_dt) & (date_col <= end_dt)
        
        return df.ix[mask] 
    
    #获取所有深市股票代码
    def get_tickersz(self,tickerall):
        return [tk for tk in tickerall if tk.startswith('0') or tk.startswith('3')]
    
    #获取所有深市股票代码（不包含创业板）
    #tp---True返回包括停牌股票，False不反悔停牌股票
    def get_tickerszmain(self,tickerall):
        return [tk for tk in tickerall if tk.startswith('0')]
    
    #获取所有创业板股票代码
    def get_tickercyb(self,tickerall):
        return [tk for tk in tickerall if tk.startswith('3')]
        
    #获取所有沪市股票代码
    def get_tickersh(self,tickerall):
        return [tk for tk in tickerall if tk.startswith('6')]
    
    #获取所有股票代码，可以选择略过停牌的股票
    #tp---True返回包括停牌股票，False不反悔停牌股票
    def get_tickerall(self,tp=True):
        field=['code']
        print '获取股票代码....'
        if tp:
            
            return self.wp.itfStoBas_proc(field=field).squeeze().tolist()
        else:
            return self.wp.itfTodAll_proc(field=field).squeeze().tolist()
    
    #获取所有沪市融资融券股票代码
    def get_tickerrzrq_sh(self):
        now=self.base.today_as_str()
        field=['stockCode']
        return self.wp.itfShMarDetA_proc(date=now,field=field).squeeze()
    
    #获取所有深市融资融券股票代码
    def get_tickerrzrq_sz(self):
        now=self.base.today_as_str()
        field=['stockCode']
        return self.wp.itfSzMarDetA_proc(date=now,field=field).squeeze()
    
    #XSHE=深圳，XSHG=上海
    #当日停牌复牌股票列表
    def get_tickerTPFP(self,tp='True',mkt=''):
        field='ticker'
        res_row_sel={'exchangeCD':mkt}
        if tp:
            return self.wp.itfSecTip_proc(field=field,res_row_sel=res_row_sel).squeeze()
        else:
            return self.wp.itfSecTip_proc(tipsTypeCD='R',field=field,res_row_sel=res_row_sel).squeeze()
        
    
    #获取沪深300股票代码和权重
    def get_hs300(self):
        field=['code','weight']
        return self.wp.itfHs300_proc(field=field)
    
    #输入股票代码返回其ts行业分类df
    def get_ts_industry(self,tickers='',field=''):
        res_row_sel={'code':tickers}
        return self.wp.itfIndCla_proc(field=field,res_row_sel=res_row_sel)
    
    #输入股票代码返回其ts概念分类df，多个概念作为一个列项
    def get_ts_concept(self,tickers='',field=''):
        res_row_sel={'code':tickers}
        data=self.wp.itfConCla_proc(field=field,res_row_sel=res_row_sel)
        data_dict=self.base.pdgrp_to_dic(data['c_name'].groupby(data['code']))
        data_df=pd.DataFrame({'code':data_dict.keys(),'c_name':data_dict.values()})
        return data_df
    
    #输入股票代码返回其通联二级行业分类df
    def get_tl_industry(self,tickers='',field=['ticker','industryName2']):
        res_row_sel={'code':tickers}
        data=self.wp.itfEquInd_proc(field=field,res_row_sel=res_row_sel)
        data_dict=self.base.pdgrp_to_dic(data['industryName2'].groupby(data['ticker']))
        data_df=pd.DataFrame({'code':data_dict.keys(),'industryName2':data_dict.values()})
        return data_df
    
    #获取某个股指的交易日线
    #所有trade类必须有date项
    def get_index_trade_day(self,code,start,end='',field=['date','close','volume'],pct=True,
                            pct_fields=[]):
        datenam='date'
        if field:
            field=self.base.lists_add(field,datenam)
        
        data=self.wp.itfHDatInd_proc(code,start,end,field)
        
        data_rev=data.iloc[::-1]
        data_rev.set_index(datenam,inplace=True)
        if pct:
            if not pct_fields:
                pct_fields=data_rev.columns
            data_rev.loc[:,pct_fields]=data_rev[pct_fields].pct_change(1)*100
        return data_rev
    
    #获取沪深融资融券汇总交易数据
    #所有trade类必须有opDate项
    def get_rzrq_trade_day(self,start,end='',field=['opDate','rzye','rzmre','rqyl','rqmcl'],
                            pct=True,pct_fields=[]):
        datenam='opDate'
        if field:
            field=self.base.lists_add(field,datenam)
        
        data=self.wp.itfShMar_proc(start,end,field)
        
        data_rev=data.iloc[::-1]
        data_rev.set_index(datenam,inplace=True)
        
        if pct:
            if not pct_fields:
                pct_fields=data_rev.columns
            data_rev.loc[:,pct_fields]=data_rev[pct_fields].pct_change(1)*100
        return data_rev
    
    #获取雅虎交易数据
    def get_YH_trade_day(self,ticker,start='',end='',field=['Date','Close','Volume'],
                         pct=True,pct_fields=[]):
        datenam='Date'
        if field:
            field=self.base.lists_add(field,datenam)
            print field
        data=self.wp.itfYHtradat_proc(ticker,start,end,field)
        data_rev=data.iloc[::-1]
        data_rev.set_index(datenam,inplace=True)
        
        data_rev=data_rev.convert_objects(convert_numeric=True)

        
        if pct:
            if not pct_fields:
                pct_fields=data_rev.columns
            data_rev.loc[:,pct_fields]=data_rev[pct_fields].pct_change(1)*100
        return data_rev
    
    #获取沪股通日流入流出数据
    #所有trade类必须有date项
    def get_dfc_hgt_day(self,start='',end='',p_max=12,field=['date','buy_amt','sell_amt','bal_today','bal_total'],
                            pct=False,pct_fields=[]):
        datenam='date'
        if field:
            field=self.base.lists_add(field,datenam)
        
        if not start:
            start='2014-01-01'
        
        data=self.wp.itfWBdfchgt_proc(p_max=p_max,field=field)
        
        data_rev=data.iloc[::-1]
        data_rev.set_index(datenam,inplace=True)
        
        data_sel=self._sel_row_bydate(data_rev,start,end)
        
        if pct:
            if not pct_fields:
                pct_fields=data_sel.columns
            data_sel.loc[:,pct_fields]=data_sel[pct_fields].pct_change(1)*100
        return data_sel


    #==========================================================#
    
        
    #【实现函数】按类别获取股票列表
    #输入所需的类别，可以是行业、概念，返回类别包含的股票列表
    #输入为空的情况，返回所有行业/概念的列表
    #返回：pandas group
    def _get_stockbygrp(self,itf,grp='',grp_index='ticker',field=[],res_row_sel={}):
        #获取行业、概念数据
        print '获取全部分类数据.....'
        ind_df=itf(field=field,res_row_sel=res_row_sel)
        #去除重复
        ind_df=ind_df.drop_duplicates(subset=grp_index)
        print '数据分组.....'
        #分组
        df_group=ind_df[field[0]].groupby(ind_df[field[1]])
        return df_group
    
    #【实现函数】按照给定的股票编码，逐个获取函数数据
    #如果股票在其中某日没有数据，则当日返回null
    #如果股票在区间内全部没有数据，则返回空的df，df列名为select_field
    #返回生成器of df
    def _get_byticker(self,tickers,itf,**itf_paras):
        if type(tickers)==str:
            tickers=[tickers]     
        for t in tickers:
            print u'获取数据:'+t+'.....'
            clopri_df=itf(t,**itf_paras)
            yield clopri_df
            
    #【实现函数】按照股票代码，逐个获取函数数据
    #适用于每个股票数据只有一行：合并行后返回df
    def _get_byticker_merge(self,tickers,itf,**itf_para):
        if type(tickers)==str:
            tickers=[tickers]  
        data_list=[itf(t,**itf_para) for t in tickers]
        df=pd.concat(data_list,axis=0) 
        df.index=tickers
        return df
            

    
    
#    #按照（通联）行业获取股票列表
#    #inds---行业中文名称，默认为全部行业
#    def get_stockbyidstry(self,inds='',lev=1):
#        #行业数据接口
#        itf=self.wp.itfEquInd_proc
#        #接口返回的分组数据，股票代码、行业名称
#        #这里只取到一级或二级
#        industryName='industryName'+str(lev)
#        field=['ticker',industryName]
#        #输入处理
#        #
#        if inds!='':
#            #将inds转为utf-8编码
#            #inds=self.base.zhs_decode(inds)
#            #设置选择的行业
#            res_row_sel={industryName:inds}
#        else:
#            #选择全部行业
#            res_row_sel={}
#        #获取行业分组
#        stock_grp=self._get_stockbygrp(itf,inds,field=field,res_row_sel=res_row_sel)
#        #将group格式转为dic格式
#        stock_grp_dic=self.base.pdgrp_to_dic(stock_grp)
#        return stock_grp_dic
#    
#    #按照概念获取股票列表
#    def stock_bycon(self,cons=''):
#        #概念数据接口
#        itf=self.wp.itfConCla_proc
#        #接口返回的分组数据，股票代码、行业名称
#        field=['code','c_name']
#        #输入处理
#        #
#        if cons!='':
#            #将inds转为utf-8编码
#            cons=self.base.zhs_decode(cons)
#            #设置选择的行业
#            res_row_sel={field[1]:cons}
#        else:
#            #选择全部行业
#            res_row_sel={}
#        #获取行业分组
#        stock_grp=self._stock_bygrp(itf,cons,field=field,res_row_sel=res_row_sel)
#        #将group格式转为dic格式
#        stock_grp_dic=self.base.pdgrp_to_dic(stock_grp)
#        return stock_grp_dic
#        
    
    #给一段日期范围，返回股票交易日期列表
    #计算方法：提取大盘指数在给定日期范围内的交易日期
    #输入：
    #start--string，格式‘2015-01-01’
    #end--string, 格式‘2015-01-01’,截止日期不填写默认为最后一个交易日
    #返回:
    #格式为pd.series
    def get_workdays(self,start,end=''):
        field=['date']
        if end=='':
            end=self.base.today_as_str()
        work_dates=self.wp.itfHDatInd_proc('399106',start=start,end=end,field=field)
        if not work_dates is None:
            try:
                return sorted(work_dates.squeeze(),reverse=False)
            except TypeError:
                return work_dates
        else:
            return None
        
    def get_workdays_last(self,date=''):
        gap=7
        if date=='':
            date=self.base.today_as_str()
        end=self.base.today_as_str(base_date=date,gap_val=-1)
        start=self.base.today_as_str(base_date=date,gap_val=-gap)
        workdays=self.get_workdays(start,end)
        if not workdays is None:
            return workdays[-1]
        else:
            return self.get_workdays_last(end)
            
        

        
    #一组股票某一天的交易数据
    def get_day_trade(self,tickers,date='',trade_field=['date','close','volume']):
        #日期默认为当天
        if not date:
            date=self.base.today_as_str()
        #获取的交易数据必须为交易日期
        if self.get_workdays(date,date) is None:
            print '输入日期非交易日期！'
            return None
            
        itf=self.wp.itfHisDatD_proc
        
        trade_data=self._get_byticker_merge(tickers,itf,start=date,end=date,field=trade_field)
        return trade_data
    
    #获取某一天全部股票的交易数据
    def get_day_trade_all(self,date='',trade_field=[]):
        print '获取最近一个交易日.....'
        last_trade_date=self.get_workdays_last(date=self.base.today_as_str())
        
        if len(trade_field)>0:
            trade_field=list(set(['code']+trade_field))
        
        
        if (pd.to_datetime(date)>=last_trade_date) or len(date)==0:
            print '获取当前交易信息.....'
            trade_data=self.wp.itfTodAll_proc(field=trade_field)
            trade_data.set_index(['code'],inplace=True)
            print '获取当前停牌列表.....'
            tp_tickers=self.get_tickerTPFP()
            tp_tickers=[self.base.int_to_str(t) for t in tp_tickers]
            tp_data=pd.DataFrame([[None]*(len(trade_field)-1)]*len(tp_tickers),index=tp_tickers,columns=trade_data.columns)
            return pd.concat([trade_data,tp_data],axis=1)
            
        else:
            tickers=self.get_tickerall(tp=True)
            return self.get_day_trade(tickers,date=date,trade_field=trade_field)
    
    #获取当日指数交易数据
    def get_day_trade_index(self,code=['000001','399106','399006','000300'],field=['code','change']):
        res_row_sel={'code':code}
        index_data=self.wp.itfIndTra_proc(field=field,res_row_sel=res_row_sel)
        index_data.set_index(['code'],inplace=True)
        return index_data
        
    
    #获取一组股票在范围日期内的交易复权日线
    def get_daytrade_stock(self,tickers,start='',end='',trade_field=['date','close','volume']):
        itf=self.wp.itfHDat_proc
        itf_paras={'start':start,'end':end,'field':trade_field}
        #获取股票交易信息
        trade_data=self._get_byticker(tickers,itf,itf_paras)
        #获取工作日列表（df）
        work_days=self.get_workdays(start,end)
        #以工作日列表为标准，合并所有交易数据df
        data_merged=self.base.merge_format(work_days,trade_data,merge_on='date',df_names=tickers)
        
        data_merged = data_merged.set_index(['date'])
        return data_merged

    def get_daytrade_concept(self,connam,date,trade_field=['date','close','volume','amount']):
        contickers=self.db_proc.get_tickerconcept(connam)
        itf=self.wp.itfHDat_proc
        itf_paras={'start':date,'end':date,'field': trade_field}
        trade_data=self._get_byticker(tickers,itf,itf_paras)
        res=stat.proc_df_addcumsum(res,calc_fields)
    
    #获取全部股票近几个月的交易数据
    def get_monthtrade_stock(self,months,tickers='',trade_field=['date','close']):
        if not tickers:
            tickers=self.get_tickerall(tp=False)
        end=self.base.today_as_str()
        start=self.base.today_as_str(end,gap_type=1,gap_val=-months)
        data=self.get_daytrade_stock(tickers,start=start,end=end,trade_field=trade_field)
        return data
    
    #获取全部股票当前价格
    def get_curtrade_stock(self,tickers='',trade_field=['code','trade']):
        res_row_sel={'code':tickers}
        trade_data=self.wp.itfTodAll_proc(field=trade_field,res_row_sel=res_row_sel)
        return trade_data
    
    #获取一组指数在范围日期内的交易日信息
    def get_daytrade_index(self,tickers=['000001','000300','399001','399006'],start='',end='',trade_field=['date','close','volume']):
        itf=self.wp.itfHDatInd_proc
        #获取指数交易信息
        trade_data=self._get_byticker(tickers,itf,start=start,end=end,field=trade_field)
        
        return trade_data
        
    #获取一组境外股票在范围日期内的交易日信息
    def get_daytrade_YH(self,tickers,start='',end='',trade_field=['Date','Close']):
        itf=self.wp.itfYHtradat_proc
        #获取指数交易信息
        trade_data=self._get_byticker(tickers,itf,start=start,end=end,field=trade_field)
        
        return trade_data
    
    #获取某一天一组股票的交易日线close
    def get_datetrade_allstock(self,tickers='',date='',trade_field=['date','close','volume']):
        if not date:
            date=self.base.today_as_str()
            
        if self.get_workdays(date,date) is None:
            print '输入日期非交易日期！'
            return None
            
        itf=self.wp.itfHisDat_proc
        
        if not tickers:
            tickers=self.get_tickerall(tp=False)
            
        trade_data=self._get_byticker(itf,tickers,start=date,end=date,select_field=trade_field)
        return trade_data
    
    #获取某一天所有股票的交易日涨跌幅，按照上海版、深圳主板、创业板返回
    def get_datetrade_stock(self,date='',trade_field=['date','close','volume']):
        if not date:
            date=self.base.today_as_str()
        start=self.get_workdays_last(date).strftime(date_format)
            
        tickerall=self.get_tickerall(tp=False)
        tickers_szmain=self.get_tickerszmain(tickerall)
        tickers_cyb=self.get_tickercyb(tickerall)
        tickers_sh=self.get_tickersh(tickerall)
        
        for tks in [tickers_sh,tickers_szmain,tickers_cyb]:
            yield self.get_daytrade_stock(tks,start,date,trade_field)


    def get_class_tickerall(self,tickers=''):
        if not tickers:
            tickers=self.get_tickerall(tp=False)
        
             
        

    
    #获取某一天所有股票的交易信息和基本信息
    def get_daytrade_infoall(self,tickers='',date='',trade_field=['date','price_change','volume']):
        if not date:
            date=self.base.today_as_str()
            
        if self.get_workdays(date,date) is None:
            print '输入日期非交易日期！'
            return None
            
        itf=self.wp.itfHisDat_proc
        itf_paras={'start':date,'end':date,'ktype':'D','field':trade_field}
        
        if not tickers:
            tickers=self.get_tickerall(tp=False)
        
        merged_df=pd.DataFrame()
        for dt,na in zip(trade_data,tickers):
            dt['ticker']=na
            merged_df=pd.concat([merged_df,dt],axis=0)
        
        #

        
        return merged_df
    
    #获取一组指数在某日的分钟线
    #sh=上证指数 sz=深圳成指 hs300=沪深300指数 sz50=上证50 zxb=中小板 cyb=创业板
    def get_mintrade_stock(self,tickers=['sh','sz','cyb','hs300'],
                           start='',end='',trade_field=['date','close','volume','turnover']):
        itf=self.wp.itfHDatInd_proc
        #获取指数交易信息
        trade_data=self._get_byticker(itf,tickers,start=start,end=end,select_field=trade_field)
        #获取工作日列表（df）
        work_days=self.get_workdays(start,end)
        #以工作日列表为标准，合并所有交易数据df
        data_merged=self.base.merge_format(work_days,trade_data,merge_on='date',df_names=tickers)
        return data_merged
    

        
        
        
        
        
        
        
    