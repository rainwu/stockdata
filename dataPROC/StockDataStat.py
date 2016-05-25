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
import numpy as np
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
    
    def _calc_outliers_std(self,data,n=3,ismask=False):
        m,s=(np.mean(data),np.std(data))
        down,up=(m-n*s,m+n*s)
        if ismask:
            return np.logical_or(data<=down, data>=up)
        else:
            return data[np.logical_or(data<=down, data>=up)]
    
    def _calc_outliers_q(self,data,n=1.5,ismask=False):
        q31=np.percentile(data,[75])-np.percentile(data,[25])
        m=np.median(data)
        down,up=(m-n*q31,m+n*q31)
        if ismask:
            return np.logical_or(data<=down, data>=up)
        else:
            return data[np.logical_or(data<=down, data>=up)]
        
    
    def calc_outliers(self,data,isnorm=True,ismask=False):
        data=np.array(data)
        data = data[~np.isnan(data)]
        
        if isnorm:
            return self._calc_outliers_std(data,ismask=ismask)
        else:
            return self._calc_outliers_q(data,ismask=ismask)
        
        
        
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
    
    
    def contrade_stat(self,df):
        size=df.shape[0]
        vol_total=df['volume'].sum()
        amt_total=df['amount'].sum()
        pchan_median=df['p_change'].median()
        pchan_mean=df['p_change'].mean()
        pchan_mad=df['p_change'].mad()
        
        #outlier分析
        #行业内outlier，和行业median比较
        outlier_mask=self.calc_outliers(df['p_change'],isnorm=False,ismask=True)
        if sum(outlier_mask)>0:
            pchan_outl=df.dropna().ix[outlier_mask]
            outliers=pchan_outl['p_change']
            #--outlier对于行业内差异的影响度：
            #------股票偏离median的值 除以 行业内所有股票偏离median的总和，再将所有outlier的占比相加
            outlier_efft=np.sum(np.abs(outliers-np.median(df['p_change'])))*100/np.sum(np.abs(df['p_change']-np.median(df['p_change'])))
            avg_efft=100*(len(outliers))/(size-1)
        
        #总体outlier，和对应的股指比较
        df['p_change_fix']=df['p_change']-df['p_change_idx']
        pchan_fix_median=df['p_change_fix'].median()
        pchan_fix_mean=df['p_change_fix'].mean()
        pchan_fix_mad=df['p_change_fix'].mad()
        
        outlier_mask=self.calc_outliers(df['p_change_fix'],isnorm=False,ismask=True)
        if sum(outlier_mask)>0:
            pchan_fix_outl=df.dropna().ix[outlier_mask]
            outliers_fix=pchan_fix_outl['p_change_fix']
            outlier_fix_efft=np.sum(np.abs(outliers_fix-np.median(df['p_change_fix'])))*100/np.sum(np.abs(df['p_change_fix']-np.median(df['p_change_fix'])))
            avg_efft_fix=100*(len(outliers_fix))/(size-1)
        
        

    #
        
        