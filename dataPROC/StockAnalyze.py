# -*- coding: utf-8 -*-
"""
Created on Wed Dec 02 14:07:18 2015

@author: warriorzhai
"""
import pandas as pd
from StockDataProc import StockDataProc
from StockBase import StockBase

roundto=2
proc=StockDataProc()
ba=StockBase()
#补充数据计算结果，自定义列名称
per_change='per_change'    #由收盘价计算涨跌幅

#对比参考基准，沪深300
index_ticker='399300'


def test():
    start='2015-11-27'
    tickers=['300244','002230']
    inds=['综合行业','食品行业']
    method='percent'
    return index_closenum(start=start,method=method)

#======数据处理：常用数据获取函数======================
#给一段日期范围，返回股票交易日期列表
#计算方法：提取大盘指数在给定日期范围内的交易日期
#输入：
#start--string，格式‘2015-01-01’
#end--string, 格式‘2015-01-01’,截止日期不填写默认为最后一个交易日
#返回:
#格式为pd.series
def get_workdays(start,end=''):
    field=['date']
    if end=='':
        end=ba.today_as_str()
    work_dates=proc.itfHDatInd_proc('399106',start=start,end=end,field=field)
    return work_dates[field[0]]




#======数据处理：基本操作函数======================
#重命名df的行名或列名
#输入：
#df--pd.dataframe
#rname--list of str,行名
#如果rname为false或空列表则不修改名字
#返回：
#修改过名字的df
def proc_df_rename(df,rname=False,cname=False):
    if rname:
        df.index=rname
    if cname:
        df.columns=cname
    return df

#按行或列计算df每列/行的变化率
#计算方向是第一行/列比第二行的变化率，因此计算后最后一行数据会是空值
#输入：
#df--所有的值必须为nmumeric
#axis--0，按照行的方向（vertical）计算变化率，1，按照列的方向（hori）计算变化率
#drop_last--是否去掉最后一个空值的行/列，True去掉
#返回：
#变化率版本的df，如果df中有不为numeric的值，返回原df
def proc_df_pct(df,axis=0,drop_last=True):
    try:
        df_perchange=df.pct_change(periods=-1,axis=axis)
    except TypeError:
        print '仅支持df中所有值全部为numeric格式'
        return df
        
    if drop_last:
        if axis==1:
            df_perchange=df_perchange.drop(df_perchange.columns[-1],axis=axis)
        else:
            df_perchange=df_perchange.drop(df_perchange.index[-1],axis=axis)
    return df_perchange




#一个df列表，以列df_init为标准，merge列表中所有df，返回merge结果
#df_list---iter of df,待合并的df列表
#df_init--list/pd.series 合并的参照列
#merge_on--每个df中合并参照的列的名称
def merge_format(df_init,df_list,merge_on,merge_mode='outer'):
    merged_df=pd.DataFrame()
    merged_df[merge_on]=df_init
    #检查df_list必须是可iter 的
    if not hasattr(df_list, '__iter__'):
        df_list=[df_list]
    
    for df in df_list:
        if not df is None:
            merged_df=pd.merge(merged_df,df,on=merge_on, how=merge_mode)
#            #为合并的列修正列名
#            new_df.columns=[t+'_'+x for x in left_col]
#            #拼接总表和行业表
#            merged_df=pd.concat([merged_df,new_df],axis=1)
    return merged_df

#将df按行名或列名抽取为若干小df字典
#direction--0按照行名抽取，1按照列名抽取
#cnames--list of row names，list长度必须大于1
def df_divide_byrname(df,names):
    div_dict={}
    for div_na in names:
        mask=[s.startswith(div_na) for s in df.index]
        div_dict[div_na]=df[mask]
    return div_dict


#按照给定的股票编码，逐个获取函数数据
#如果股票在其中某日没有数据，则当日返回null
#如果股票在区间内全部没有数据，则返回空的df，df列名为select_field
#返回生成器of df
def _get_byticker(itf,tickers,start='',end='',select_field=['date','close','volume']):
    if type(tickers)==str:
        tickers=[tickers]        
    for t in tickers:
        print u'获取数据:'+t+'.....'
        clopri_df=itf(t,start=start,end=end,field=select_field)
        if clopri_df is None:
            print t+u':无数据！'
            clopri_df=pd.DataFrame(columns=select_field)
        yield clopri_df
        
#获取给定股票、指数，在指定时间范围内的收盘价日线,成交量
tickers=['002230','002223','300244','002672','300144','002292','002444','002707']
end='2016-01-29'
start='2016-01-01'
#返回df
def stock_tradedata(tickers,start='',end='',trade_field=['date','close','volume']):
    itf=proc.itfHDat_proc
    #获取股票交易信息
    trade_data=_get_byticker(itf,tickers,start=start,end=end,select_field=trade_field)
    #获取工作日列表（df）
    work_days=get_workdays(start,end='')
    #以工作日列表为标准，合并所有交易数据df
    data_merged=merge_format(work_days,trade_data,merge_on='date')
    return data_merged

def data_proc(data,itfs,paras):
    for it,pa in zip(itfs,paras):
        data=it(data,**pa)
    return data
        


    


        


#将pandas group转为字典格式
def pdgrp_to_dic(pdgrp):
    pdgrp_dic={}
    for na,val in pdgrp:
        print '按类别获取股票列表......'
        try:
            pdgrp_dic[na]=val.tolist()
        except:
            pdgrp_dic[na]=val
            continue
    return pdgrp_dic


#按类别获取股票列表
#输入所需的类别，可以是行业、概念，返回类别包含的股票列表
#输入为空的情况，返回所有行业/概念的列表
#返回：pandas group
def _stock_bygrp(itf,inds='',field=[],res_row_sel={}):
    #获取行业、概念数据
    print '获取全部数据.....'
    ind_df=itf(field=field,res_row_sel=res_row_sel)
    #去除重复
    ind_df=ind_df.drop_duplicates(subset='ticker')
    print '数据分组.....'
    #分组
    df_group=ind_df[field[0]].groupby(ind_df[field[1]])
    return df_group





    

#按照（通联）行业获取股票列表
def stock_byind(inds='',lev=1):
    #行业数据接口
    itf=self.wp.itfEquInd_proc
    #接口返回的分组数据，股票代码、行业名称
    #这里只取到一级或二级
    industryName='industryName'+str(lev)
    field=['ticker',industryName]
    #输入处理
    #
    if inds!='':
        #将inds转为utf-8编码
        inds=zhs_decode(inds)
        #设置选择的行业
        res_row_sel={industryName:inds}
    else:
        #选择全部行业
        res_row_sel={}
    #获取行业分组
    stock_grp=_stock_bygrp(itf,inds,field,res_row_sel)
    #将group格式转为dic格式
    stock_grp_dic=pdgrp_to_dic(stock_grp)
    return stock_grp_dic

#按照概念获取股票列表
def stock_bycon(cons=''):
    #概念数据接口
    itf=proc.itfConCla_proc
    #接口返回的分组数据，股票代码、行业名称
    field=['code','c_name']
    #输入处理
    #
    if cons!='':
        #将inds转为utf-8编码
        cons=zhs_decode(cons)
        #设置选择的行业
        res_row_sel={field[1]:cons}
    else:
        #选择全部行业
        res_row_sel={}
    #获取行业分组
    stock_grp=_stock_bygrp(itf,cons,field,res_row_sel)
    #将group格式转为dic格式
    stock_grp_dic=pdgrp_to_dic(stock_grp)
    return stock_grp_dic

#按照地区获取股票列表
def stock_byarea(areas=''):
    pass

#输入行业名称，获取起下属二级行业
def get_subinds(ind_name):
    field=['industryName1','industryName2']
    res_row_sel={'industryName1':ind_name}
    ind_names=proc.itfEquInd_proc(field=field,res_row_sel=res_row_sel)
    if ind_names.empty:
        print '没有找到输入行业的二级行业！'
        return None
    else:
        return ind_names.drop_duplicates()
    

#获取所有股票的基本信息
#TS基本信息
def get_stockinfo_all():
    field_ts=['code','name','area','totals','outstanding','totalAssets',
           'esp','bvps','timeToMarket']
    field_tl=['ticker','exchangeCD','nonrestfloatA']
    field_tl_gem=['code']
    field_tl_trade=['code','trade']
    #TS基本数据
    print '获取TS源股票基本数据....'
    stockinfo=proc.itfStoBas_proc(field=field_ts)
    #补充计算出股东权益、负债率、毛利润
    stockinfo['totalEquity']=stockinfo['bvps']*stockinfo['totals']
    stockinfo['totalDebt_per']=((stockinfo['totalAssets']-stockinfo['totalEquity'])/stockinfo['totalAssets']).round(roundto)
    stockinfo['profitGross']=stockinfo['esp']*stockinfo['totals']
    
    #创业板股票列表
    #如果是创业板，则值为1
    print '获取创业板股票列表....'
    stockgem_list=proc.itfGemCla_proc(field=field_tl_gem)
    stockgem_list['is_cyb']=pd.Series([1]*len(stockgem_list.iloc[:,0]))
    
    #股票最新价格，为了计算当前市值
    print '获取股票当前交易价格....'
    stock_trade=proc.itfTodAll_proc(field=field_tl_trade)

    
    #通联基本数据
    print '获取通联源股票基本数据....'
    stockinfo_add=proc.itfEqu_proc(field=field_tl)
    
    print '数据格式整理....'
    #将表的列名统一
    stockinfo.rename(columns={ 'code' : 'ticker'},inplace=True)
    stockgem_list.rename(columns={ 'code' : 'ticker'},inplace=True)
    stock_trade.rename(columns={ 'code' : 'ticker'},inplace=True)
    #将合并索引列的格式统一：TS ticker格式为string，通联 ticker格式为int
    #都转为int
    stockinfo_add['ticker']=stockinfo_add['ticker'].apply(lambda x:ba.int_to_str(x,6))
    #合并两个表，以‘ticker’为参照
    merged_df=pd.merge(stockinfo,stockinfo_add,on='ticker')
    merged_df=pd.merge(merged_df,stockgem_list,on='ticker',how='outer')
    merged_df=pd.merge(merged_df,stock_trade,on='ticker')
    
    #去除重复的ticker信息，主要原因是当前成交价表中含有重复
    merged_df=merged_df.drop_duplicates(subset='ticker')
    
    #补充计算流通市值
    merged_df['liquid_value']=merged_df['trade']*merged_df['outstanding']
    return merged_df

#获取三个大盘在时间范围内的价格变动
def index_closenum(tickers=['000001','399106','399006'],start='',end=''):
    left_col=[per_change,'volume']
    itf=proc.itfHDatInd_proc
    prices=_get_closeprice(itf,tickers,start=start,end=end)
    work_days=get_workdays(start,end='')
    prices_merged=merge_format(work_days,prices,merge_on='date',left_col=left_col)
    return prices_merged


#获得每个行业的股票的在时间范围内的收盘价/涨幅，日线
def stockclose_byind(inds='',start='',end='',method='percent'):
    stock_ind_trades={}
    #获取每个行业的股票列表
    stock_group=stock_byind(inds=inds)
    for ind_name,ind_tickers in stock_group.items():
        print '按行业获取股票交易数据......'
        ind_trade=stock_closeprice(ind_tickers,start=start,end=end,method=method)
        stock_ind_trades[ind_name]=ind_trade
    return stock_ind_trades
    #return stock_group.apply(lambda x:stock_closeprice(x.tolist(),start=start,end=end))


#某行业在某段时间范围的均涨跌幅
def perchange_byind(inds='',start='',end=''):
    #获取各个行业的包含股票的交易数据
    print 
    stock_ind_trades=stockclose_byind(inds,start=start,end=end)
    ind_avg=pd.DataFrame()
    #计算每个行业在时间范围内的均值
    ind_avg['date']=get_workdays(start,end)
    for n,d in stock_ind_trades.items():
        ind_avg[n+'mean']=d.iloc[:,1:].mean(axis=1)
        ind_avg[n+'median']=d.iloc[:,1:].median(axis=1)
        ind_avg[n+'std']=d.iloc[:,1:].std(axis=1)
    return ind_avg

def write_dic_csv(dic):
    for n,d in dic.items():
         proc.write_df_csv(d,n)


    
    
def main():
    #获取全部股票基本信息
    stocks_info=get_stockinfo_all()
    proc.write_df_csv(stocks_info,u'股票信息表')
    #将基本信息按照行业分类，写入文档
    #获取各个一级行业的股票列表
    stks_byind=stock_byind()
    route="stockinfobyind1\hy_"
    #获取各个2级行业的股票列表
    stks_byind=stock_byind(lev=2)
    route="stockinfobyind2\hy_"
    #将各个行业的股票信息写入文档
    for na,tic_list in stks_byind.items():
        na=zhs_decode(na)
        proc.write_df_csv(stocks_info.loc[stocks_info['ticker'].isin(tic_list)],route+na)
    
    #获取交易数据的返回项
   
    #大盘近一个月的涨跌幅
    #设置日期
    end=ba.today_as_str()
    start=ba.today_as_str(gap=-30)
    workdays=get_workdays(start,end)
    #获取大盘交易数据
    index_trade_per=index_closenum(start=start,end=end)
    #数据格式处理
    index_trade_per=index_trade_per.iloc[:index_trade_per.shape[0]-1,:]
    proc.write_df_csv(index_trade_per,u'近一个月指数涨跌幅')
    
    
    
    trade_field=['date','close','volume']
    div_names=trade_field[1:]
    #所有行业一个月均涨跌幅
    #测试用：采掘行业的交易数据
    tickers=stks_byind['采掘']
    trade_data=stock_tradedata(tickers,start=start,end=end,trade_field=trade_field)
    data_trans=trade_data.transpose()
    #将按照行名div_names抽取，每个行名单独一个表
    div_data=df_divide_byrname(data_trans,div_names)
    #定义每种表的处理过程
    close_proc_itfs=[proc_df_rename,proc_df_pct]
    close_proc_paras=[{'rname':tickers,'cname':workdays},{'axis':1}]

    
    volume_proc_itfs=[proc_df_rename]
    volume_proc_paras=[{'rname':tickers,'cname':workdays}]
    
    
    close_data=data_proc(div_data[div_names[0]],close_proc_itfs,close_proc_paras)
    volume_data=data_proc(div_data[div_names[1]],volume_proc_itfs,volume_proc_paras)
    
    
    
    
    all_trade_per=all_trade_per.iloc[:all_trade_per.shape[0]-1,:]
    route="stocktradebyind1\hy_"
    proc.write_df_csv(all_trade_per,'stocktradebyind1\caijue')
    
    #将各个行业的股票交易数据写入文档
    route="stocktradebyind1\hy_"
    all_trade_per=stock_closeprice(stocks_info['ticker'],start=start,end=end)
    for na,tic_list in stks_byind.items():
        na=zhs_decode(na)
        
        trade_per=trade_per.iloc[:trade_per.shape[0]-1,:]
        proc.write_df_csv(trade_per,na)
    #将基本信息写入文档
#    proc.write_df_csv(stocks_info,u'股票信息表')
    
#行业涨跌幅统计
#start='2015-09-30'
#method='percent'
#index_perchange=index_closenum(start=start,method=method)
#proc.write_df_csv(index_perchange,u'指数涨跌幅')
#ind_perchange=perchange_byind(start=start)
#proc.write_df_csv(ind_perchange,u'行业涨跌幅')
#proc.write_df_csv(ind_perchange.transpose(),u'行业涨跌幅V2')
#proc.write_df_csv(index_perchange.transpose(),u'指数涨跌幅V2')
#概念涨跌幅统计


#新闻热度统计

##000792西藏矿业研究
#field=['date','close','volume']
#xzky=proc.itfHDat_proc('000762',start='2015-01-01',field=field)
#xzky['close']=xzky['close'].pct_change(-1)*100
#sz=proc.itfHDatInd_proc('399106',start='2015-01-01',field=field)
#sz['close']=sz['close'].pct_change(-1)*100
#merged_df=pd.merge(sz,xzky,on='date')
#proc.write_df_csv(merged_df,u'西藏矿业对比大盘')