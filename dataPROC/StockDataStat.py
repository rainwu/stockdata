# -*- coding: utf-8 -*-
"""
Created on Wed Feb 03 11:47:49 2016

@author: warriorzhai
"""
import os
try:
    os.chdir(os.environ["WORK_PATH"])
except KeyError:
    pass
import settings
from Base import Base 
#from dataPROC.StockDataProc import StockDataProc


class StockDataStat(object):
    
    def __init__(self,token=settings.token):
        self.token=settings.token
        self.ba=Base()
       # self.proc=StockDataProc()
    
    #对某些列进行计算后作为新的列加入df
    def _df_calcadd(self,df,colnams,new_colid,calc_op,calc_op_para={}):
        new_cols=calc_op(df[colnams],**calc_op_para)
        new_colnams=[n+new_colid for n in colnams]
        df[new_colnams]=new_cols
        return df
    
    def calc_cumsum(self,df,axis=0):
        return df.cumsum(axis=axis)
    
    def proc_df_addcumsum(self,df,colnams):
        new_colid='cum'
        calc_op=self.calc_cumsum
        return self._df_calcadd(df,colnams,new_colid,calc_op)
        
        
    #按行或列计算df每列/行的变化率
    #计算方向是第一行/列比第二行的变化率，因此计算后最后一行数据会是空值
    #输入：
    #df--所有的值必须为nmumeric
    #axis--0，按照行的方向（vertical）计算变化率，1，按照列的方向（hori）计算变化率
    #drop_last--是否去掉最后一个空值的行/列，True去掉
    #返回：
    #变化率版本的df，如果df中有不为numeric的值，返回原df
    def proc_df_pct(self,df,axis=0,drop_last=True,periods=1):
        para=0.5*periods/abs(periods)
        try:
            df_perchange=df.pct_change(periods=periods,axis=axis)
        except TypeError:
            print '仅支持df中所有值全部为numeric格式'
            return df
            
        if drop_last:
            if axis==1:
                df_perchange=df_perchange.drop(df_perchange.columns[para-0.5],axis=axis)
            else:
                df_perchange=df_perchange.drop(df_perchange.index[para-0.5],axis=axis)
        return df_perchange
        
    def calc_valrank(self,vals,basedf):
        ranks=[]
        for v,i in zip(vals,basedf):
            if v:
                ranks=ranks+self.ba.get_val_rank(v,basedf[i])
            else:
                ranks=ranks+None
        return ranks
        
        
        
        
        
        
        