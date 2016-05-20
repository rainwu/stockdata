# -*- coding: utf-8 -*-
"""
Created on Tue Apr 19 15:37:57 2016

@author: warriorzhai
"""
#index默认在最前面

#control
control_table_struct={
    'collnam':'control',
    'itemnams':['collnam','step','update'],
    'itemvals':[0,None]
    }



#stockinfo
itemnams=['ticker','name','industryName1','industryName2',
    'concepts','area','outstanding', 'totals','timeToMarket']
stockinfo_table_struct={
    'collnam':'stockinfo',
    'itemnams':itemnams,
    'itemvals':[None,[],[],[],None,None,None,None],
    'keyindex':0
    }
stockinfo_crawlnew_struct={
    'ts_StoBas':['code','name','area'],
    'db_StoBas':['ticker','name','area'],
    'ts_ConCla':['code','c_name'],
    'db_ConCla':['ticker','concepts'],
    'tl_EquInd':['ticker','industryName1','industryName2'],
    'db_EquInd':['ticker','industryName1','industryName2'],
}
stockinfo_crawldaily_struct={
    'ts_StoBas':['code','outstanding', 'totals','timeToMarket'],
    'db_StoBas':['ticker','outstanding', 'totals','timeToMarket']
}

#stockgrps
stockgrps_table_struct={
    'collnam':'stockgrps',
    'itemnams':['grpnam'],
    'itemvals':[],
    'indexvals':['tickers','concepts'],
    'keyindex':0
    }
    
stockgrps_crawlnew_struct={
    'ts_ConCla':['code','c_name'],
    'db_ConCla':['ticker','grpnam'],
    'tl_EquInd':['ticker','industryName2'],
    'db_EquInd':['ticker','grpnam']
}
stockgrps_crawldaily_struct={
   
}

#stockhgt
stockhgt_table_struct={
    'collnam':'stockhgt',
    'itemnams':['date', "bal_total","buy_amt","sell_amt","bal_today"],
    'keyindex':0
    }

#stockholders  待议
stockholders_table_struct={
    'collnam':'stockholders',
    'itemnams':['ticker','holders','holders_lt','holders_fd','holders_td'],
    'itemvals':[[],[],[],[]]
    }
    




tables_all=[control_table_struct['collnam'],
            stockinfo_table_struct['collnam'],
            stockgrps_table_struct['collnam']]