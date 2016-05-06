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
#���ļ����սӿ����ݽ����Զ�����μӹ�������Ҫ���к��еĳ�ȡ


class StockInterfaceWrap(object):
    
    def __init__(self,token=settings.token):
        self.token=token
        self.ba=Base()
        self.api=StockInterfaceTS()
        self.apiyh=StockInterfaceYH()
        self.apiwb=StockInterfaceWB()
    

    #��dataframe�У���ȡ������������
    def _df_rowselect(self,df,row_sel={},row_between_ops=[]):
        bool_operater={
        'and': lambda a,b: a&b,
        'or':lambda a,b: a|b
        }
        
        try:
                #ȥ���ֵ��еĿ�ֵ
            row_sel=dict([(i,row_sel[i]) for i in row_sel.keys() if len(row_sel[i])>0])
        except AttributeError:
            pass
        
        if len(row_sel)==0:
            return df
        
        
            
        
        
        
        if len(row_between_ops)==0:
            row_between_ops=['and']*(len(row_sel)-1)
        
        
        if len(row_sel)-1!=len(row_between_ops):
             print '�������У�����֮���߼���ϵ���Ź�ϵ����Ӧ��'
             return False
        

            
        m,n=df.shape
        
        #��ʼ��ѡ��bool
        mask_df=pd.Series(data=[True]*m)
        row_between_ops=['and']+row_between_ops
        
        for (k,v),op in zip(row_sel.items(),row_between_ops):
            try:
                if type(v)==list or type(v)==tuple:
                    mask_col=df[k].isin(v)
                else:
                    mask_col=(df[k]==v)
            except TypeError:
                print '������������ֵtype����'
                sys.exit()

            mask_df=bool_operater[op](mask_df,mask_col)
        
        return df[mask_df]
        
    
    #��StockInterfaceTS�У���ȡ�Ľӿ����ݽ��м򵥴���
    #��ȡָ�����У�������ĳ��������
    #�еĹ���ֻ�ܴ���=�������
    #����˵����
    #api_itf---StockInterfaceTS�нӿں�������
    #api_itf_paras--StockInterfaceTS�нӿں����Ĳ������ֵ��ʽ
    #res_col_sel--���ر���ѡȡ��������list of strings
    #res_row_sel--���ر��У�ɸѡ�е��������ֵ䣬{'����':��ֵ}
    #row_between_ops--���res_row_sel֮����߼���ϵ����'and' 'or'���֣�Ĭ��ȫand
    #���ӣ�
    #��ȡ����Ϊ'000001'��Ʊ��20151101~20151110�ĸ�Ȩ�������ݣ�
    #��������high=100��low=10,������ֻҪclose
    #api_itf=api.getHDat
    #api_itf_paras={'code':'000001','start':'2015-11-01','end':'2015-11-10'}
    #res_col_sel=['close']
    #res_row_sel={'high':100,'low':10}
    #row_between_ops=['or']
    #_itfdata_proc(api_itf,api_itf_paras,res_col_sel,res_row_sel,
    #row_between_ops)
    #����
    #������dataframe����None
    def _itfdata_proc(self,api_itf,api_itf_paras={},res_col_sel=[],res_row_sel={},
                      row_between_ops=[],date_proc=False):
        
        #��ȡ�ӿ����ݣ�dataframe
        data=api_itf(**api_itf_paras)
        
        
        #��ȡ����Ϊ��
        #û������shape==(0,1)����
        #�ޣ�ֱ�ӷ���
        if data is None or data.shape==(0,1):
            return data
        
        #�����ݵ�row index��ȡ��Ϊ��
        if not data.index is None:
            #Ϊindex������ƣ�Ĭ��Ϊ��rownum��
            if data.index.name is None:
                data.index.name='rownum'
            data.reset_index(inplace=True)

        #��ѡ��
        data=self._df_rowselect(data,res_row_sel,row_between_ops)
        
        
        
        
        
        #��ѡ��
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
            
        
    
    
        
    
#=================������Ϣ===========================

    #��ȡ��Ʊ����
    #�ӿ��������
    #�ӿ�����
#    code����Ʊ����
#    name����Ʊ����
#    c_name����������
    #����
    #dataframe
    def itfConCla_proc(self,field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getConCla
        res=self._itfdata_proc(api_itf,res_col_sel=field,res_row_sel=res_row_sel,
                      row_between_ops=row_between_ops)
        return res
    
    #��Ʊ��ҵ����TS�汾
    def itfIndCla_proc(self,field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getIndCla
        res=self._itfdata_proc(api_itf,res_col_sel=field,res_row_sel=res_row_sel,
                      row_between_ops=row_between_ops)
        return res
    
    #��Ʊ��ҵ����ͨ���汾
    def itfEquInd_proc(self,field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getEquInd
        res=self._itfdata_proc(api_itf,res_col_sel=field,res_row_sel=res_row_sel,
                      row_between_ops=row_between_ops)
        return res
        
    #��Ʊ����������TS
    def itfStoBas_proc(self,field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getStoBas
        res=self._itfdata_proc(api_itf,res_col_sel=field,res_row_sel=res_row_sel,
                      row_between_ops=row_between_ops)
        return res
        
    #��Ʊ��������ͨ���汾
    def itfEqu_proc(self,ticker='',equTypeCD='A',field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getEqu
        api_itf_paras={'equTypeCD':equTypeCD,'ticker':ticker}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)
        return res
        
    #��ȡ��ҵ���Ʊ�б�
    def itfGemCla_proc(self,field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getGemCla
        res=res=self._itfdata_proc(api_itf,res_col_sel=field,res_row_sel=res_row_sel,
                      row_between_ops=row_between_ops)
        return res
    
    #��ȡ����300��Ʊ�б��Ȩ��  
    def itfHs300_proc(self,field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getHs300
        res=res=self._itfdata_proc(api_itf,res_col_sel=field,res_row_sel=res_row_sel,
                      row_between_ops=row_between_ops)
        return res
        
    #ָ��������Ϣ
    def itfIndex_proc(self,ticker='',secID='',field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getIndex
        api_itf_paras={'ticker':ticker,'secID':secID}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)
        return res
    
    #����ͣ�Ƹ�����ҵ��Ϣ
    def itfSecTip_proc(self,tipsTypeCD='H',field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getSecTips
        api_itf_paras={'tipsTypeCD':tipsTypeCD}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)
        return res

#=============�¼���Ϣ===================================
    #ȫ���¹���Ϣ
    def itfNewSto_proc(self,field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getNewSto
        res=self._itfdata_proc(api_itf,res_col_sel=field,res_row_sel=res_row_sel,
                      row_between_ops=row_between_ops)
        return res
    
    #����ֹ�����
    def itfFudHod_proc(self,year='',quater='',field=[],res_row_sel={},
                       row_between_ops=[]):
        api_itf=self.api.getFudHod
        api_itf_paras={'year':year,'quater':quater}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)
        return res
        

#============��������====================================
    #���и�Ȩ��������
    #�ӿ�������
#    code:string,��Ʊ���� e.g. 600848
#    start:string,��ʼ���� format��YYYY-MM-DD Ϊ��ʱȡ��ǰ����
#    end:string,�������� format��YYYY-MM-DD Ϊ��ʱȡȥ�����
#    index:Boolean���Ƿ��Ǵ���ָ����Ĭ��ΪFalse
    #�ӿ�����
#    date : �������� (index)
#    open : ���̼�
#    high : ��߼�
#    close : ���̼�
#    low : ��ͼ�
#    volume : �ɽ���
#    amount : �ɽ����
    #����
    #dataframe
    def itfHDat_proc(self,code,start='',end='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getHDat
        api_itf_paras={'code':code,'start':start,'end':end}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)     
        if res.shape[0]==0:
            print code+u':�����ݣ�'            
            res.loc[0]=[None]*res.shape[1]
        return res
    
    #ָ����Ȩ��������
    #����
    #dataframe
    def itfHDatInd_proc(self,code,start='',end='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getHDat
        api_itf_paras={'code':code,'start':start,'end':end,'index':True}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)     
        return res


    #���н������ݲ���Ȩ
    def itfHisDatD_proc(self,code,start='',end='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getHisDat
        api_itf_paras={'code':code,'start':start,'end':end,'ktype':'D'}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)    
        if res.shape[0]==0:
                print code+u':�����ݣ�'
                res.loc[0]=[None]*res.shape[1]
        return res


    #�����շ��ӽ�������
    def itfHisDat_proc(self,code,start='',end='',ktype='5',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getHisDat
        api_itf_paras={'code':code,'start':start,'end':end,'ktype':ktype}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)    
        if res.shape[0]==0:
                print code+u':�����ݣ�'
                res.loc[0]=[None]*res.shape[1]
        return res
    
    #�����ճɽ����
    def itfTikDat_proc(self,code,date='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getTikDat
        api_itf_paras={'code':code,'date':date}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)     
        return res
    
    #����������ȯͳ����Ϣ
    def itfSzMar_proc(self,start,end='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getSzMar
        api_itf_paras={'start':start,'end':end}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)     
        return res
    
    #����������ȯͳ����Ϣ
    def itfShMar_proc(self,start,end='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getShMar
        api_itf_paras={'start':start,'end':end}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)    
        return res
    
    #����������ȯ,��ֻ��Ʊ��ϸ
    def itfShMarDetS_proc(self,symbol,start,end='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getShMarDetS
        api_itf_paras={'symbol':symbol,'start':start,'end':end}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)    
        return res
    
    #����������ȯ,���й�Ʊ������ϸ
    def itfShMarDetA_proc(self,date='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getShMarDetA
        api_itf_paras={'date':date}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)    
        return res
    
    #����������ȯ,���й�Ʊ������ϸ
    def itfSzMarDetA_proc(self,date='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getSzMarDetA
        api_itf_paras={'date':date}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)    
        return res
  
    #��ǰ���й�Ʊ��ʵʱ������Ϣ
    def itfTodAll_proc(self,tp=True,field=[],res_row_sel={},row_between_ops=[]):
        api_itf=self.api.getTodAll
        res=self._itfdata_proc(api_itf,res_col_sel=field,res_row_sel=res_row_sel,
                      row_between_ops=row_between_ops)
        return res
    
        #��ǰ���й�Ʊ��ʵʱ������Ϣ
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
    
    #��ȡ�����Ʊ��һ��ʱ�������
    def itfYHtradat_proc(self,ticker,start,end='',field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.apiyh.get_tradedata
        api_itf_paras={'ticker':ticker,'start':start,'end':end}
        res=self._itfdata_proc(api_itf,api_itf_paras,field,res_row_sel,
                      row_between_ops)
#        res.columns=self.ba.to_lower(res.columns)
        return res

    #����ͨÿ������������
    def itfWBdfchgt_proc(self,field=[],
                     res_row_sel={},row_between_ops=[]):
        api_itf=self.apiwb.get_dfcf_hgt
        res=self._itfdata_proc(api_itf,res_col_sel=field,res_row_sel=res_row_sel,
                      row_between_ops=row_between_ops)
        return res

        
        
        
        
        