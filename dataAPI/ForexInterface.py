# -*- coding: utf-8 -*-
"""
Created on Mon Jan 11 10:37:01 2016

@author: warriorzhai
"""
import socks
import socket
socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, "127.0.0.1", 1080)
socket.socket = socks.socksocket

import urllib2
import json
import sys
import pandas as pd
import numpy as np
import datetime
import xmltodict
from sklearn import linear_model
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import math
#各个接口网址
fixer_url_template='http://api.fixer.io/%s?symbols=%s&base=%s'
yahoo_url_template='http://query.yahooapis.com/v1/public/yql?q=select * from yahoo.finance.xchange where pair in (%s)&env=store://datatables.org/alltableswithkeys'

#网址格式化
yahoo_url_safe='/*,:?()=&'



#虚拟货币篮子
nr_currency=['btc','ltc']
retry=3



def today_as_str(date_format="%Y-%m-%d",gap=0):
            #获取当前日期datetime.datetime格式
    now = datetime.datetime.now()
            #转化为string格式
    tar_date=now+datetime.timedelta(gap)
    return tar_date.strftime(date_format)


            #将一个字典列表中，所有指定key的值导出
            #为防万一，使用如果key不存在于某个字段会报错
            #参数说明：
            #key--指定要查找的key值
            #diclist--一个dict的列表，如[{dic1},{dic2}]
            #返回：
            #dict[key]的列表
def unpack_dic(key,diclist):
                #为防万一，使用如果key不存在于某个字段会报错
    try:
        res=[i[key] for i in diclist]
    except KeyError:
            print '字典中key值不存在'
            sys.exit()

    return res




#移除全部为val的行
def remov_df_row(df,val=0,axis=1):
    return df.loc[~(df==val).all(axis=axis)]
    
    #将dataframe写入当前工作路径的csv
    #参数说明
    #df--数据
    #filename--文件名
def write_df_csv(df,file_name):     
    today=today_as_str()
        #文件名格式为
        #指定file_name_今日日期.csv
    file_name=file_name+'_'+today+'.csv'
    df.to_csv(file_name+'.csv',encoding='utf-8')
    
start='2016-01-01'
#给出起始日期和结束日期，返回其之间的日期的数组
#返回：
#list of strings
def get_dateseq(start,end='',workdays=False):
    if len(end)==0:
        end=today_as_str()
    if workdays:
        date_datetime=pd.bdate_range(start,end).to_pydatetime()
    else:
        date_datetime=pd.date_range(start,end).to_pydatetime()
    datetime_to_str=np.vectorize(lambda x:x.strftime('%Y-%m-%d'))
    return datetime_to_str(date_datetime)

def df_adsum(df,start='',end=''):
    rnum=df.shape[0]
    if not start:
        start=0
    if start<0:
        start=rnum+start
    if not end:
        end=rnum
    if end<0:
        end=end+rnum
    add_range=rnum-start
    list_addsum=[df.ix[start:start+i].sum() for i in range(add_range+1)]
    df_addsum=pd.concat(list_addsum, axis=1).transpose()
#    df_addsum.index=['']+df.index[start:end]
    return df_addsum

#sin（α－β）＝sinαcosβ－cosαsinβ
#cos（α－β）＝cosαcosβ＋sinαsinβ
def rotate_ma(deg):
    ra=math.radians(deg)
    ma=np.matrix([[math.cos(ra),math.sin(ra),],[-math.sin(ra),math.cos(ra)]])
    #ma=np.matrix([[math.cos(ra),-math.sin(ra)],[math.sin(ra),math.cos(ra)]])
    return np.round(ma,6)


#每列数据的slope
def df_slope(df):
    x=range(df.shape[0])
    ks=[np.polyfit(x,df[col],1)[0] for col in df]
    return np.degrees(np.arctan(ks))

def _get_url(url):
    for i in range(retry):
        try:
            response = urllib2.urlopen(url)
            res= response.read()
        except:
            print '连接失败，重试第'+str(i)+'次...'
            continue
        break
    if i==(retry-1):
        print '数据获取失败，程序结束'
        print '请检查：1.是否开启vpn。 2.是否输入参数错误。'
        sys.exit()
    return res

#获取所有货币对
def get_all_currents():
    url='http://api.fixer.io/latest'
    res=_get_url(url)
    print '获取所有货币种类'
    try:
        forex_json=json.loads(res)['rates']
    except KeyError:
        print '无数据'
        return None
    return forex_json.keys()+['EUR']
   


#def write_fgs(pdf_name,plt_names):
#    pp = PdfPages(pdf_name+'pdf')
#    for i in fgs:
#        pp.savefig(i)
#        plt.close()

#def multi_figs(df,pdf_name):
#    pdf_obj = PdfPages(pdf_name+'.pdf')
#    for col in df:
#        fig = plt.figure()
#        plt.plot(df[col])
#        plt.title(col)
#        pdf_obj.savefig(fig)
#    pdf_obj.close()

#
#def linear_model(df):
#    rnum=df.shape[0]
#    fit_x=pd.DataFrame(range(rnum))
#    fit_y=df.iloc[:,2].tolist()
#    plt.plot(fit_x, fit_y)
#    regr = np.polyfit(fit_x,fit_y,2)
#    regr.fit(fit_x,fit_y)
#    regr.score(fit_x,fit_y)

def plot_single(x,y='',title='main'):
    fig = plt.figure()
    plt.title(title)
    if y:
        plt.plot(x,y)
    else:
        plt.plot(x)
    return fig

def plot_multi(x,y='',title='main',cor_dic={}):
    fig = plt.figure()
    plt.title(title)
    
    if y:
        plt.gca().set_color_cycle([cor_dic[k] for k in y.columns])
        for col in y:
            plt.plot(x,y[col])
        plt.hlines(0,0,y.shape[0],linestyles='dotted')
        plt.legend(y.columns, loc='upper left')
        
    else:
        plt.gca().set_color_cycle([cor_dic[k] for k in x.columns])
        for col in x:
            plt.plot(x[col])
        print x.shape
        plt.hlines(0,0,x.shape[0],linestyles='dotted')
        plt.legend(x.columns, loc='upper left')
    return fig

def read_cfvs(route,names):
    f_name=[route+na for na in names]
    f_list=[]
    print f_name
    for fn in f_name:
        try:
            f_list.append(pd.read_csv(fn+'.csv',index_col=0))
        except IOError:
            print '文件不存在'
            continue
    return f_list
    
    
#返回一天的，以某货币为base的，若干货币的汇率（字典格式）
#date
#返回：
#dic
#若没有数据返回空列表
def _get_forex_1d(symbol,date='',base='CNY'):
    symbol_format=','.join(symbol)
    
    if len(date)==0:
        date=today_as_str()
    
    
    url=fixer_url_template % (date,symbol_format,base)

    forex_str= _get_url(url)
    forex_json=json.loads(forex_str)
    print '获取'+date+'数据成功...'
    
    try:
        forex_dic=forex_json['rates']
    except KeyError:
        print '无数据'
        return None
    return forex_dic


def _get_forex_now(symbol,base='CNY'):
    cb_func=lambda x,y:'"'+x+y+'"'
    symbol_pairs=[cb_func(base,s) for s in symbol]
    symbol_input=','.join(symbol_pairs)
    
    url=yahoo_url_template % symbol_input
    url_format=urllib2.quote(url,safe=yahoo_url_safe)
    
    forex_str= _get_url(url_format)
    
    try:
        forex_dict_raw=xmltodict.parse(forex_str)['query']['results']['rate']
    except KeyError:
        print '无数据'
        return None
    
#    forex_dict_keys=unpack_dic('@id',forex_dict_raw)
    forex_dict_values=unpack_dic('Rate',forex_dict_raw)
    #获取格式为unicode，转为float'
    forex_dict_values=[float(x) for x in forex_dict_values]
    forex_df=pd.DataFrame([forex_dict_values],index=[today_as_str()],columns=symbol)
    
    return  forex_df
    
    

#输入一组货币列表，返回fixer中此货币列表的顺序
def get_fixer_symbol(symbol,base):
    print 'before last:'+str(len(symbol))
    forex_dic=_get_forex_1d(symbol,base=base)
    print 'last:'+str(len(forex_dic.keys()))
    if forex_dic:
        return forex_dic.keys()
    else:
        return forex_dic

#返回一天的，以某货币为base的，若干货币的汇率
def get_fixer_rate(symbol,date='',base='CNY'):
    forex_dic=_get_forex_1d(symbol,date,base)
    if forex_dic:
        return forex_dic.values()
    else:
        return forex_dic
    

#获取若干实体货币对base货币的，任意时间段的汇率日线/日增长率
def get_forexrates_R(symbol,base,start,end='',per=True):
    date_seq=get_dateseq(start,end)
    forex_data=map(lambda dt:get_fixer_rate(symbol,dt,base),date_seq)
    forex_names=get_fixer_symbol(symbol,base=base)
    print len(forex_names)
    forex_df=pd.DataFrame(forex_data,columns=forex_names,index=date_seq)
    forex_pct=forex_df.pct_change(1)*1000
    
    if per:
        return forex_pct.loc[~(forex_pct==0).all(axis=1)]
    else:
        return forex_df.loc[~(forex_pct==0).all(axis=1)]



#获取当日外汇分钟线，暂时没有数据源
def get_forexmratesmin_R(symbol):
    pass

#获取当前实时汇率
def get_forexratesnow_R(symbol,base='CNY'):
     print '获取实时汇率数据...'
     forex_df=_get_forex_now(symbol,base=base)
     return forex_df
    
    #处理输入格式
    #合并symble、base为雅虎接口要求格式
    
def get_forex_day(base,currencys,start,end):
    print '获取'+base+'货币日线数据...'
    currency=filter(lambda a: a != base, currencys)
    forex_data=get_forexrates_R(currency,base=base,start=start,end=end,per=False)
    forex_data_now=get_forexratesnow_R(currency,base=base)
    forex_data_tt=pd.concat([forex_data,forex_data_now])
    forex_data_per=forex_data_tt.pct_change(1)*1000
    forex_data_per=forex_data_per.ix[1:]
    return forex_data_tt,forex_data_per


def forex_stat(df_per):
    week=5
    rnum=df_per.shape[0]
    keys=[u'日涨跌',u'周涨跌',u'月涨跌',u'月涨天数占比',u'月涨跌波动',u'月涨跌均值', \
    u'月涨最大',u'月跌最大']
    day_change=df_per.iloc[-1,:]
    week_change=df_per.iloc[-week,:]
    mon_change=df_per.iloc[0,:]
    mon_ups=(df_per>=0).sum()/rnum
    mon_var=df_per.var()
    mon_avg=df_per.median()
    mon_max=df_per.max()
    mon_min=df_per.min()
    vals=[day_change,week_change,mon_change,mon_ups,mon_var,mon_avg,mon_max,mon_min]
    df_stat=pd.DataFrame.from_items(zip(keys,vals)).transpose()
    df_stat.columns=df_per.columns
    return df_stat

def save_cur_plots(df_generator,currencys,base,route,pdf_name,cor_dic):
    pdf_obj = PdfPages(route+pdf_name+'.pdf')
    
    for cu in currencys:
        print cu+'...'
        cpr_df=pd.DataFrame([df[cu] for df in df_generator if cu in df.columns]).transpose()
        col_name=filter(lambda a: a != cu, base)
        cpr_df.columns=col_name
        fig=plot_multi(cpr_df,title=cu,cor_dic=cor_dic)
        pdf_obj.savefig(fig)
    pdf_obj.close()
    
#获取任一对虚拟货币的任意时间段的汇率日线
#def get_forexrates_V(symbol,base,start,end):
#    pass


#获取某天的货币组合矩阵(套利迷宫)





def updaily():
    #观察实体货币篮子
    base=['CNY','USD','EUR','JPY']
    cor=['red','yellow','blue','green']
    cor_dic=dict(zip(base,cor))
    currencys=get_all_currents()
    end=today_as_str(gap=-1)
    start=today_as_str(gap=-31)
    route='forex\\'
    for x in base:
        forex_data,forex_data_per=get_forex_day(x,currencys,start,end)
        forex_data.to_csv('forex\month_'+x+'.csv',encoding='utf-8')
        forex_data_per.to_csv('forex\month_'+x+'_per.csv',encoding='utf-8')
    
    f_names=['month_'+b+'_per' for b in base]
    df_mon_list=read_cfvs(route,f_names)
    
    pdf_name=u'汇率近一月走势'+today_as_str()
    #月累计涨幅
    forex_addsum_mon=[df_adsum(df) for df in df_mon_list]
    save_cur_plots(forex_addsum_mon,currencys,base,route,pdf_name,cor_dic)
    map(lambda a,b:a.to_csv('forex\month_'+b+'_addsum.csv',encoding='utf-8'),
        forex_addsum_mon,base)

    pdf_name=u'汇率走势日报'+today_as_str()
    #近五天累计涨幅
    forex_addsum_week=[df_adsum(df,start=-5) for df in df_mon_list]
    save_cur_plots(forex_addsum_week,currencys,base,route,pdf_name,cor_dic)
    map(lambda a,b:a.to_csv('forex\week_'+b+'_addsum.csv',encoding='utf-8'),
        forex_addsum_week,base)
    
    #走势分析
    ex=forex_addsum_mon[0]
    x=np.array(range(ex.shape[0]))
    ks=df_slope(ex)
    res=[(rotate_ma(k)*np.matrix([x,np.array(ex[col])]))[1,:] for col,k in zip(ex,ks)]
    stds=[np.std(x) for x in res]
    temp=pd.DataFrame([ks,stds],columns=ex.columns,index=['cny_slope','cny_std']).transpose()
    temp.to_csv('forex\month_ana.csv',encoding='utf-8')
    
    

    
# 
def test():
    base=['CNY','USD','EUR','JPY']
    route='forex\\'
    f_names=['month_'+b+'_per' for b in base]
    df_mon_list=read_cfvs(route,f_names)
    forex_addsum_mon=[df_adsum(df) for df in df_mon_list]
    temp=forex_addsum_mon[0]
    x=np.array(range(temp.shape[0]))
    y=np.array(temp.iloc[:,0])
    k,m=np.polyfit(x,y,1)
    
    ax2 = plt.subplot(111)
    ax2.scatter(x,y1)
    ax2.plot(x,k*x+m-m)
    ax2.set_xlim([-10,50])
    ax2.set_ylim([-10,50])
    rad=np.degrees(np.arctan(k))
    rm=rotate_ma(rad)
    res1=rm*np.matrix([x,y1])
    res2=rm*np.matrix([x,k*x+m-m])


    ax1 = plt.subplot(111)
    ax1.scatter(np.array(res1[0,:])[0],np.array(res1[1,:])[0])
    ax1.plot(np.array(res2[0,:])[0],np.array(res2[1,:])[0])
    ax1.set_xlim([-10,50])
    ax1.set_ylim([-10,50])
    ax1.show()
    
    
    rm=rotate_ma(-rad)
    res3=rm*res1
    ax3 = plt.subplot(111)
    ax3.scatter(np.array(res3[0,:])[0],np.array(res3[1,:])[0])
    ax3.plot(np.array(res2[0,:])[0],np.array(res2[1,:])[0])
    ax3.set_xlim([-10,50])
    ax3.set_ylim([-10,50])
    #比特币在岸人民币价格
    
    #
    #主要实体货币对人民币汇率近一个月