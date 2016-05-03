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
import databaseAPI.db_tables as tables

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