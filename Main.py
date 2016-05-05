# -*- coding: utf-8 -*-
"""
Created on Mon Apr 25 13:26:52 2016

@author: warriorzhai
"""
import os
try:
    os.chdir(os.environ["WORK_PATH"])
except KeyError:
    pass
from databaseAPI.DatabaseInit import DatabaseInit
from dataPROC.StockDataProc import StockDataProc
from dataPROC.StockDataStat import StockDataStat
from dataAPI.StockInterfaceYH import StockInterfaceYH
import databaseAPI.db_tables as tables


#数据抓取部分
instance=DatabaseInit()
#instance.insert_stockinfo_StoBas()
#instance.insert_stockinfo_EquInd()
#instance.insert_stockinfo_ConCla()
#instance.init_stockgrps()
db_table=tables.stockgrps_table_struct
crawl_table=tables.stockgrps_crawlnew_struct
crawl_field=crawl_table['tl_EquInd']
crawl_field_2db=crawl_table['db_EquInd']
grpnam=db_table['indexvals'][1]
instance.update_stockgrps_tickers(db_table,grpnam)
instance.update_stockgrps_concepts(db_table,crawl_field,crawl_field_2db,grpnam)
instance.update_stockgrps_inds2(db_table,crawl_field,crawl_field_2db,grpnam)


#数据提取部分
start='2014-08-01'
end='2016-05-01'
ins=StockDataProc()
stat=StockDataStat()
#三大a股指数交易数据
calc_fields=['close']
pct_fields=['close']

code='000300'
res=ins.get_index_trade_day(code,start,end,pct_fields=pct_fields)
res=stat.proc_df_addcumsum(res,calc_fields)
res.to_csv('000300hz.csv',encoding='utf-8')

code='000001'
res=ins.get_index_trade_day(code,start,end,pct_fields=pct_fields)
res=stat.proc_df_addcumsum(res,calc_fields)
res.to_csv('000001sh.csv',encoding='utf-8')

code='399006'
res=ins.get_index_trade_day(code,start,end,pct_fields=pct_fields)
res=stat.proc_df_addcumsum(res,calc_fields)
res.to_csv('399006cyb.csv',encoding='utf-8')

code='399001'
res=ins.get_index_trade_day(code,start,end,pct_fields=pct_fields)
res=stat.proc_df_addcumsum(res,calc_fields)
res.to_csv('399001sz.csv',encoding='utf-8')

#融资融券
res=ins.get_rzrq_trade_day(start,end,pct=False)
res.to_csv('rzrq.csv',encoding='utf-8')

#MSCI新兴市场指数
ticker='EEM'
yh=StockInterfaceYH()
res=yh.get_tradedata(ticker,start,end)
res.to_csv('yh.csv',encoding='utf-8')

#重要概念：沪港通、次新股、石油、煤炭、钢铁

#沪港通流入流出

#股指期货，期权

#人民币在岸、离岸兑美元交易数据

#人民币对一揽子指数

#日本、美国、欧洲股指，MSCI等
#






