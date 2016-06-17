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
import sys
import numpy as np
import itertools
import logging
import multiprocessing as mp
from multiprocessing import JoinableQueue,Queue,Process
import threading
from Base import Base 
from dataAPI.StockInterfaceWrap import StockInterfaceWrap
from dataPROC.StockDataStat import StockDataStat
from databaseAPI.DatabaseInterface import DatabaseInterface
from databaseAPI.DatabaseProc import DatabaseProc
from dataPROC.MultiProcessTask import MultiProcessTask,MultiThreadTask


logger=logging.getLogger(__name__)
logger.setLevel(logging.INFO)# create a file handler
handler=logging.FileHandler('test.log')
handler.setLevel(logging.INFO)# create a logging format
formatter=logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)# add the handlers to the logger
logger.addHandler(handler)

date_format=settings.date_format
process_num=settings.process_num
thread_num=settings.thread_num

class StockDataProc(object):
    
    def __init__(self,token=settings.token):
        self.token=settings.token
        self.base=Base()
        self.wp=StockInterfaceWrap()
        self.stat=StockDataStat()
        self.db_proc=DatabaseProc()
        
    #==============批量数据抓取处理==============               
    def _get_results(self,resultqueue):
        while not resultqueue.empty():
                yield resultqueue.get()
        
            
    def _put_tasks(self,tasks,taskqueue,taskqueue_lk):
        taskqueue_lk.acquire()
        try:
            #将数据塞入队列
            for i in tasks:
                taskqueue.put(i)
            taskqueue_lk.notify()
        finally:       
            taskqueue_lk.release()
    
    def _start_threads(self,taskqueue,resultqueue,taskqueue_lk,threadnum):
        for p in range(threadnum):
            p = threading.Thread(target=self.multithread_task, args=(taskqueue,resultqueue,
                                         taskqueue_lk),
                                         name='TH'+str(p))
            p.daemon=True
            p.start()
            
    def multiprocess_task(self,taskqueue,resultqueue,taskqueue_lk,
                          threadnum):    
        #判断taskqueue是否为空的锁，因为queue的empty方法没有加锁，所以手工加锁
        taskqueue_lk_th = threading.Condition(threading.Lock())
        
        #等待taskqueue放入完毕
        taskqueue_lk.acquire()
        if taskqueue.empty():
             logger.info(multiprocessing.current_process().name+' wait!')
             taskqueue_lk.wait()
        taskqueue_lk.release()
        
        #开始线程，数据抓取的执行部分
        self._start_threads(taskqueue,resultqueue,taskqueue_lk_th,threadnum)
            
        #进程等待所有线程结束
        logger.info(multiprocessing.current_process().name+' join!')
        taskqueue.join()
        logger.info(multiprocessing.current_process().name+' end!')
            
            
    def multithread_task(self,taskqueue,resultqueue,taskqueue_lk):
        func_dict={'itfHDat_proc':self.wp.itfHDat_proc,
        'itfHisDatD_proc':self.wp.itfHisDatD_proc
                   }
        #执行数据抓取直到taskqueue所有task被执行完
        while 1:
            #taskqueue中没有任务时候，线程挂起
            #等待taskqueue放入完毕
            logger.info(threading.current_thread().name+' start!')
            taskqueue_lk.acquire()
            if taskqueue.empty():
                logger.info(threading.current_thread().name+' wait!')
                taskqueue_lk.wait()
            taskqueue_lk.release()
        
            #数据抓取，queue的get函数有锁
            logger.info(threading.current_thread().name+' getdata!')
            task_func,task_funcparas=taskqueue.get()
            print task_func,task_funcparas
            data=func_dict[task_func](**task_funcparas)
            #数据写入，queue的put函数有锁
            logger.info(threading.current_thread().name+' writedata!')
            resultqueue.put(data)
            #taskqueue的任务计数器减1，减到0释放上一级进程的join
            taskqueue.task_done()
        logger.info(threading.current_thread().name+' end!')
    
    def getdata_multiprocess(self,task_funcs,task_funcsparas,
                             processnum=None,threadnum=2):
        def _start_processes(taskqueue,resultqueue,taskqueue_lk,processnum,threadnum):
            for i in range(processnum):
                p = Process(target=self.multiprocess_task, args=(taskqueue,resultqueue,
                                         taskqueue_lk,threadnum),name='P'+str(i))
                p.daemon=True
                p.start()
                
        processnum=processnum if processnum else multiprocessing.cpu_count()
        #任务传送queue
        taskqueue=JoinableQueue()
        #任务写入/唤醒lock
        taskqueue_lk = multiprocessing.Condition(multiprocessing.Lock())
        #结果传送queue
        resultqueue=Queue()
        
        _start_processes(taskqueue,resultqueue,taskqueue_lk,
                            processnum,threadnum)
        #放入任务，唤醒进程
        self._put_tasks(zip(task_funcs,task_funcsparas),taskqueue,taskqueue_lk)
        logger.info('main join!')
        taskqueue.join()
        logger.info('main end!')
        return self._get_results(resultqueue)
    
    def getdata_multithread(self,task_funcs,task_funcsparas,
                             thread_maxnum=4):
                
        threadnum=min(len(task_funcs),thread_maxnum)
        #任务传送queue
        taskqueue=JoinableQueue()
        #任务写入/唤醒lock
        taskqueue_lk = multiprocessing.Condition(multiprocessing.Lock())
        #结果传送queue
        resultqueue=Queue()
        
        self._start_threads(taskqueue,resultqueue,taskqueue_lk,
                            threadnum)
        #放入任务，唤醒进程
        self._put_tasks(zip(task_funcs,task_funcsparas),taskqueue,taskqueue_lk)
        
        taskqueue.join()

        return self._get_results(resultqueue)
    
    
    
    def test(self):
        #wp=StockInterfaceWrap()
#        date='2016-06-01'
#        trade_field=[['volume','amount'],['p_change']]
#        itfs=['itfHDat_proc','itfHisDatD_proc']
#        
#        itfparas=[{'code':'000001','start':date,'end':date,'field': trade_field[0]},
#                      {'code':'000001','start':date,'end':date,'field': trade_field[1]}]
#            
#        mergeby='date'
    
        #res=self.getdata_multisource(itfs,itfparas,mergeby)
        date='2016-06-01'
        tickers=['000001','000002','300133','000718','600547']
        trade_field=[['volume','amount'],['p_change']]
        itfs=['itfHDat_proc','itfHisDatD_proc']
        
        itfparas=[{'start':date,'end':date,'field': trade_field[0]},
                  {'start':date,'end':date,'field': trade_field[1]}]
        
        mergebys='date'
        
        iterlen=len(tickers)
#        iterfunc_extend=[iterfunc]*paralen
#        iterfunc_paras_extend=self.base.addkey_2dictlist([iterfunc_paras]*paralen,
#                                                   iterkeynam,iterkeys)
        
        iterfunc=itertools.repeat(ex.getdata_multisource,iterlen)
        iter_para_funcs=itertools.repeat(itfs,iterlen)
        f_combineparas=lambda x,y :ex.base.addkey_2dictlist(x,
                                                   'code',y)
        iter_para_funcparas=f_combineparas(itertools.repeat(itfparas,iterlen)) if type(itfparas)==dict else map(f_combineparas,itertools.repeat(itfparas,iterlen),tickers)
        iterpara=zip(iter_para_funcs,iter_para_funcparas)
        
        res=self.getdata_iter(tickers,'code',iterfunc,iterfunc_paras)
        
        
        print res
#        taskqueue=JoinableQueue()
#        taskqueue.put(self.func_warp('itfHDat_proc',{'start':'2016-06-10'}))

        
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
    
    def getdata_multisource(self,itfs,itfparas,mergeby):

        if not self.base.is_iter(itfs):
            return itfs(**itfparas)
        
        if len(itfs)!=len(itfparas):
            print '函数列表和其参数列表长度不等!'
            sys.exit()
        
        #从所要求的接口抓取数据
        data_iter=self.getdata_multithread(itfs,itfparas)
        
        #统一数据的合并字段
        data_list=[data.set_index(mergeby) if not data.index.name==mergeby else data for data in data_iter]
        
        return pd.concat(data_list,axis=1)
    
    

    def getdata_iter(self,taskiterkeys,taskiterkeynam,taskfuncs,taskfuncs_paras,handle_iter=0):
        
        def dataiter_concat(dataiter):
            return pd.concat(dataiter)
        
        def dataiter_tocsv(dataiter):
            pass
        
        handle_itermethod=[dataiter_concat,dataiter_tocsv]
        usemethod=handle_itermethod[handle_iter]
        
        #参数处理
        iterlen=len(taskiterkeys)
#        iterfunc_extend=[iterfunc]*paralen
#        iterfunc_paras_extend=self.base.addkey_2dictlist([iterfunc_paras]*paralen,
#                                                   iterkeynam,iterkeys)
        
        iterfunc=itertools.repeat(self.getdata_multisource,iterlen)
        iter_para_funcs=itertools.repeat(iterfuncs,iterlen)
        iter_para_funcparas=self.base.addkey_2dictlist(itertools.repeat(taskfuncs_paras,iterlen),
                                                   taskiterkeynam,taskiterkeys)
        iterpara=zip(iter_para_funcs,iter_para_funcparas)
        
        self.getdata_multisource(self,iterfunc,iterpara,'date')
        dataiter=self.getdata_multiprocess(iterfunc_extend,iterfunc_paras_extend)
        return usemethod(dataiter)



    #test  itfHisDatD_proc(self,code,start='',end='',field   
        #itfHDat_proc(self,code,start='',end='',field
#    itfs=[ex.wp.itfHisDatD_proc,ex.wp.itfHDat_proc]
#    itfparas=[{'code':'000001','start':'2016-05-18','field':['date','p_change']},{'code':'000001','start':'2016-05-18','field':['date','volume','amount']}]
#    mergebys='date'
#    _get_dataandmerge(itfs,itfparas,mergebys)
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
    
    def is_tickersh(self,tickers):
        return [tk.startswith('6') for tk in tickers]
    
    def is_tickersz(self,tickers):
        return [tk.startswith('0') for tk in tickers]
    
    def is_tickercyb(self,tickers):
        return [tk.startswith('3') for tk in tickers]
    
    
    
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
    def get_index_trade_day(self,code,start,end='',field=['date','volume'],pct=True,
                            pct_fields=[]):
        
        data=self.wp.itfHDatInd_proc(code,start,end,field)
        
        data_rev=data.iloc[::-1]
        data_rev.set_index(datenam,inplace=True)
        if pct:
            if not pct_fields:
                pct_fields=data_rev.columns
            data_rev.loc[:,pct_fields]=data_rev[pct_fields].pct_change(1)*100
        return data_rev
    
    def get_tradedate_indexs(self,code,date,field=['p_change']):
        return self.wp.itfHisDatD_proc(code,date,date,field)
    
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



    def get_datetrade_concept(self,connam,date):
        trade_field=[['volume','amount'],['p_change']]
        itfs=[self.wp.itfHDat_proc,self.wp.itfHisDatD_proc]
        
        itfparas=[{'start':date,'end':date,'field': trade_field[0]},
                  {'start':date,'end':date,'field': trade_field[1]}]
        
        mergebys='date'
        
        contickers=self.db_proc.get_tickerconcept(connam)
        
        trade_data=self._get_data_iter(contickers,itfs,itfparas,mergebys)
        
        trade_df=pd.concat(trade_data)
        trade_df.index=contickers
        
        trade_df['p_change_idx']=self.get_datetrade_ticksec(contickers,date)       
        
        return trade_df

    def get_datetrade(self,date):
        trade_field=[['volume','amount'],['p_change']]
        itfs=[self.wp.itfHDat_proc,self.wp.itfHisDatD_proc]
        
        itfparas=[{'start':date,'end':date,'field': trade_field[0]},
                  {'start':date,'end':date,'field': trade_field[1]}]
        
        mergebys='date'
        
        tickers=self.db_proc.get_tickerall()
        
        #trade_data=self._get_data_iter(tickers,itfs,itfparas,mergebys)
        trade_data=self._get_data_mulprocess(itfs,itfparas,
                             mergebys,tickers)
        
        trade_df=pd.concat(trade_data)
        trade_df.index=tickers
            
        return trade_df
    
 
    def get_datetrade_ticksec(self,tickers,date):
        codes=['000001','399001','399006']
        index_data=[self.get_tradedate_indexs(x,date).squeeze() for x in codes]
        index_func=[self.is_tickersh,self.is_tickersz,self.is_tickercyb]
        return sum(map(lambda x,y:
                np.array(x(tickers))*y,index_func,index_data))
    
        
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
        work_dates_list=work_dates.index
        if not work_dates_list.name is None:
            return sorted(work_dates_list,reverse=False)
        else:
            return None
        
    def get_workdays_last(self,date=''):
        gap=7
        if date=='':
            date=self.base.today_as_str()
        end=self.base.today_as_str(base_date=date,gap_val=1)
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
    
import multiprocessing
class GetDataProcess(multiprocessing.Process):    
    def __init__(self, task_queue, result_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        
    def run(self,func,*args,**kwargs):
        proc_name = self.name
        while True:
            next_index_para = self.task_queue.get()
            if next_index_para is None:
                # Poison pill means shutdown
                print '%s: Exiting' % proc_name
                break
            print '%s: %s' % (proc_name, next_index_para)
            self.result_queue.put(func(next_index_para,*args,**kwargs))
        return 

def f(t):
    print t
      
def test():
    tickers=['000001','000002','601888','002707','600585','300009','603789','600519',
             '002027','600028']
    tasks = multiprocessing.Queue()
    results = multiprocessing.Queue()
    
    num_process = multiprocessing.cpu_count() *2
    print 'Creating %d consumers' % num_process
    processes = [ GetDataProcess(tasks, results)
                      for i in xrange(num_process) ]
    for w in processes:
        w.start()
        
    for i in tickers:
        tasks.put(i)    
            
    for w in processes:
        w.join()

import time
class Task(object):
    def __init__(self, a, b):
        self.a = a
        self.b = b
    def __call__(self):
        time.sleep(0.1) # pretend to take some time to do the work
        return '%s * %s = %s' % (self.a, self.b, self.a * self.b)
    def __str__(self):
        return '%s * %s' % (self.a, self.b)
        
    