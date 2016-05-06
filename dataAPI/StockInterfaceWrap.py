# -*- coding: gbk -*-
"""
Created on Fri Nov 13 14:17:13 2015

@author: saisn
"""
import pandas as pd
import sys
import os
try:
    os.chdir(os.environ["WORK_PATH"])
except KeyError:
    pass
from Base import Base
from dataAPI.StockInterfaceTS import StockInterfaceTS
from dataAPI.StockInterfaceYH import StockInterfaceYH
from dataAPI.StockInterfaceWB import StockInterfaceWB
import settings
#此文件接收接口数据进行自定义二次加工处理，主要是行和列的抽取


class StockInterfaceWrap(object):
    
    def __init__(self,token=settings.token):
        self.token=token
        self.ba=Base()
        self.api=StockInterfaceTS()
        self.apiyh=StockInterfaceYH()
        self.apiwb=StockInterfaceWB()
    

    #从dataframe中，抽取满足条件的行
    def _df_rowselect(self,df,row_sel={},row_between_ops=[]):
        bool_operater={
        'and': lambda a,b: a&b,
        'or':lambda a,b: a|b
        }
        
        try:
                #去除字典中的空值
            row_sel=dict([(i,row_sel[i]) for i in row_sel.keys() if len(row_sel[i])>0])
        except AttributeError:
            pass
        
        if len(row_sel)==0:
            return df
        
        
            
        
        
        
        if len(row_between_ops)==0:
            row_between_ops=['and']*(len(row_sel)-1)
        
        
        if len(row_sel)-1!=len(row_between_ops):
             print '操作的列，与列之间逻辑关系符号关系不对应！'
             return False
        

            
        m,n=df.shape
        
        #初始化选择bool
        mask_df=pd.Series(data=[True]*m)
        row_between_ops=['and']+row_between_ops
        
        for (k,v),op in zip(row_sel.items(),row_between_ops):
            try:
                if type(v)==list or type(v)==tuple:
                    mask_col=df[k].isin(v)
                else:
                    mask_col=(df[k]==v)
            except TypeError:
                print '输入行条件的值type错误！'
                sys.exit()

            mask_df=bool_operater[op](mask_df,mask_col)
        
        return df[mask_df]
        
    
    #对StockInterfaceTS中，获取的接口数据进行简单处理
    #抽取指定的列，和满足某条件的行
    #行的过滤只能处理‘=’的情况
    #参数说明：
    #api_itf---StockInterfaceTS中接口函数名称
    #api_itf_paras--StockInterfaceTS中接口函数的参数，字典格式
    #res_col_sel--返回表中选取的列名。list of strings
    #res_row_sel--返回表中，筛选行的条件，字典，{'列名':行值}
    #row_between_ops--多个res_row_sel之间的逻辑关系，有'and' 'or'两种，默认全and
    #例子：
    #获取代码为'000001'股票在20151101~20151110的复权交易数据，
    #满足条件high=100或low=10,返回列只要close
    #api_itf=api.getHDat
    #api_itf_paras={'code':'000001','start':'2015-11-01','end':'2015-11-10'}
    #res_col_sel=['close']
    #res_row_sel={'high':100,'low':10}
    #row_between_ops=['or']
    #_itfdata_proc(api_itf,api_itf_paras,res_col_sel,res_row_sel,
    #row_between_ops)
    #返回
    #处理后的dataframe，或None
    def _itfdata_proc(self,api_itf,api_itf_paras={},res_col_sel=[],res_row_sel={},
                      row_between_ops=[],date_proc=False):
        
        #获取接口数据，dataframe
        data=api_itf(**api_itf_paras)
        
        
        #获取数据为：
        #没有数据shape==(0,1)，或
        #无，直接返回
        if data is None or data.shape==(0,1):
            return data
        
        #将数据的row index提取出为列
        if not data.index is None:
            #为index添加名称，默认为‘rownum’
            if data.index.name is None:
                data.index.name='rownum'
            data.reset_index(inplace=True)

        #抽选行
        data=self._df_rowselect(data,res_row_sel,row_between_ops)
        
        
        
        
        
        #抽选列
        if len(res_col_sel)>0:
            if date_proc:
                col=map(str.lower, data.columns)
                sel=list(set(['date']+map(str.lower, res_col_sel)))
                col_sel=[col.index(s) for s in sel]
                data=data[col_sel].iloc[::-1]
            else:
                print data.columns
                print res_col_sel
                data=data[res_col_sel]
            
        return data
            
        
    
    
        
    
#=================基本信息===========================

    #获取股票概念
    #接口输入项：无
    #接口输出项：
#    code：股票代码
#    name：股票名称
#    c_name：概念名称
    #返回
    #dataframe
    def itfConCla_proc(self,field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getConCla
        res=self._itfdata_proc(api_itf,res_col_sel=field,res_row_sel=res_row_sel,
                      row_between_ops=row_between_ops)
        return res
    
    #股票行业分类TS版本
    def itfIndCla_proc(self,field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getIndCla
        res=self._itfdata_proc(api_itf,res_col_sel=field,res_row_sel=res_row_sel,
                      row_between_ops=row_between_ops)
        return res
    
    #股票行业分类通联版本
    def itfEquInd_proc(self,field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getEquInd
        res=self._itfdata_proc(api_itf,res_col_sel=field,res_row_sel=res_row_sel,
                      row_between_ops=row_between_ops)
        return res
        
    #股票基本面数据TS
    def itfStoBas_proc(self,field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getStoBas
        res=self._itfdata_proc(api_itf,res_col_sel=field,res_row_sel=res_row_sel,
                      row_between_ops=row_between_ops)
        return res
        
    #股票基本数据通联版本
    def itfEqu_proc(self,ticker='',equTypeCD='A',field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getEqu
        api_itf_paras={'equTypeCD':equTypeCD,'ticker':ticker}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)
        return res
        
    #获取创业板股票列表
    def itfGemCla_proc(self,field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getGemCla
        res=res=self._itfdata_proc(api_itf,res_col_sel=field,res_row_sel=res_row_sel,
                      row_between_ops=row_between_ops)
        return res
    
    #获取沪深300股票列表和权重  
    def itfHs300_proc(self,field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getHs300
        res=res=self._itfdata_proc(api_itf,res_col_sel=field,res_row_sel=res_row_sel,
                      row_between_ops=row_between_ops)
        return res
        
    #指数基本信息
    def itfIndex_proc(self,ticker='',secID='',field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getIndex
        api_itf_paras={'ticker':ticker,'secID':secID}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)
        return res
    
    #今日停牌复牌企业信息
    def itfSecTip_proc(self,tipsTypeCD='H',field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getSecTips
        api_itf_paras={'tipsTypeCD':tipsTypeCD}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)
        return res

#=============事件信息===================================
    #全部新股信息
    def itfNewSto_proc(self,field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getNewSto
        res=self._itfdata_proc(api_itf,res_col_sel=field,res_row_sel=res_row_sel,
                      row_between_ops=row_between_ops)
        return res
    
    #基金持股数据
    def itfFudHod_proc(self,year='',quater='',field=[],res_row_sel={},
                       row_between_ops=[]):
        api_itf=self.api.getFudHod
        api_itf_paras={'year':year,'quater':quater}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)
        return res
        

#============交易数据====================================
    #股市复权日线数据
    #接口输入项
#    code:string,股票代码 e.g. 600848
#    start:string,开始日期 format：YYYY-MM-DD 为空时取当前日期
#    end:string,结束日期 format：YYYY-MM-DD 为空时取去年今日
#    index:Boolean，是否是大盘指数，默认为False
    #接口输出项：
#    date : 交易日期 (index)
#    open : 开盘价
#    high : 最高价
#    close : 收盘价
#    low : 最低价
#    volume : 成交量
#    amount : 成交金额
    #返回
    #dataframe
    def itfHDat_proc(self,code,start='',end='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getHDat
        api_itf_paras={'code':code,'start':start,'end':end}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)     
        if res.shape[0]==0:
            print code+u':无数据！'            
            res.loc[0]=[None]*res.shape[1]
        return res
    
    #指数复权日线数据
    #返回
    #dataframe
    def itfHDatInd_proc(self,code,start='',end='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getHDat
        api_itf_paras={'code':code,'start':start,'end':end,'index':True}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)     
        return res


    #股市交易数据不复权
    def itfHisDatD_proc(self,code,start='',end='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getHisDat
        api_itf_paras={'code':code,'start':start,'end':end,'ktype':'D'}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)    
        if res.shape[0]==0:
                print code+u':无数据！'
                res.loc[0]=[None]*res.shape[1]
        return res


    #股市日分钟交易数据
    def itfHisDat_proc(self,code,start='',end='',ktype='5',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getHisDat
        api_itf_paras={'code':code,'start':start,'end':end,'ktype':ktype}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)    
        if res.shape[0]==0:
                print code+u':无数据！'
                res.loc[0]=[None]*res.shape[1]
        return res
    
    #股市日成交逐笔
    def itfTikDat_proc(self,code,date='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getTikDat
        api_itf_paras={'code':code,'date':date}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)     
        return res
    
    #深市融资融券统计信息
    def itfSzMar_proc(self,start,end='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getSzMar
        api_itf_paras={'start':start,'end':end}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)     
        return res
    
    #沪市融资融券统计信息
    def itfShMar_proc(self,start,end='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getShMar
        api_itf_paras={'start':start,'end':end}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)    
        return res
    
    #沪市融资融券,单只股票明细
    def itfShMarDetS_proc(self,symbol,start,end='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getShMarDetS
        api_itf_paras={'symbol':symbol,'start':start,'end':end}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)    
        return res
    
    #沪市融资融券,所有股票当天明细
    def itfShMarDetA_proc(self,date='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getShMarDetA
        api_itf_paras={'date':date}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)    
        return res
    
    #深市融资融券,所有股票当天明细
    def itfSzMarDetA_proc(self,date='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getSzMarDetA
        api_itf_paras={'date':date}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)    
        return res
  
    #当前所有股票的实时交易信息
    def itfTodAll_proc(self,tp=True,field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getTodAll
        res=self._itfdata_proc(api_itf,res_col_sel=field,res_row_sel=res_row_sel,
                      row_between_ops=row_between_ops)
        return res
    
        #当前所有股票的实时交易信息
    def itfIndTra_proc(self,field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getIndTra
        res=self._itfdata_proc(api_itf,res_col_sel=field,res_row_sel=res_row_sel,
                      row_between_ops=row_between_ops)
        return res

    def itfMktEqu_proc(self,ticker='',beginDate='',endDate='',tradeDate='',
                       field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getMktEqu
        api_itf_paras={'ticker':ticker,'beginDate':beginDate,'endDate':endDate,
                       'tradeDate':tradeDate}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)
        return res
    
    
    def itfFund_proc(self,ticker='',etfLof='',listStatusCd='L',secID='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getFund
        api_itf_paras={'ticker':ticker,'secID':secID,'etfLof':etfLof,'listStatusCd':listStatusCd}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)
        return res

    def itfIdxCons_proc(self,ticker='',intoDate='',secID='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getIdxCons
        api_itf_paras={'ticker':ticker,'secID':secID,'intoDate':intoDate}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)
        return res
    
    #获取境外股票的一段时间的日线
    def itfYHtradat_proc(self,ticker,start,end='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.apiyh.get_tradedata
        api_itf_paras={'ticker':ticker,'start':start,'end':end}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)
#        res.columns=self.ba.to_lower(res.columns)
        return res

    #沪股通每日余额，流入流出
    def itfWBdfchgt_proc(self,field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.apiwb.get_dfcf_hgt
        res=self._itfdata_proc(api_itf,res_col_sel=field,res_row_sel=res_row_sel,
                      row_between_ops=row_between_ops)
        return res

        
        
        
        
        