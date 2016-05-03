# -*- coding: utf-8 -*-
"""
Created on Thu Apr 07 15:31:25 2016

@author: warriorzhai
"""

import scrapy
import pandas as pd
import re
from spyder.items import TablesItem
from databaseAPI.DatabaseUpdate import DatabaseUpdate
import databaseAPI.db_tables as tables



scrapy_domain="www.askci.com"
scrapy_start_url=["http://www.askci.com/news/finance/2014/11/10/9420o1lv_all.shtml/"]
table_mark='//div[@class="newsdetailcontext"]//table[@class="ke-zeroborder"][1]//tr'
up=DatabaseUpdate()
#table_mark='//div[@class="newsdetailcontext"]//table[@class="ke-zeroborder"][1]'
class TablesSpider(scrapy.Spider):
    name = "tables"
    allowed_domains = [scrapy_domain]
    start_urls = scrapy_start_url
    
    


    def parse(self, response):
        #self.logger.warning(response.xpath(table_mark).extract())
        #数据库写入配置
        db_table=tables.stockgrps_table_struct
        grpnam=db_table['indexvals'][1]
        
        tickers=[sel.xpath('td/text()').extract()[1] for sel in response.xpath(table_mark)][1:]
        tickers_format=[re.search(r'\d+', s).group() for s in tickers]
        up.update_stockgrps(db_table,grpnam,['沪港通'],[tickers_format])

#            item = TablesItem()
#            item['ticker'] = sel.xpath('td/text()').extract()[1]
#            yield item
