
import copy
import sys
sys.path.append("..")
from future_mysql import dbBase
import os

from sqlalchemy import Column, Integer, String, Float
from sqlalchemy import Table

class GlobalVar(object):
    
    def __init__(self):
        self.basic_path = r'E:\autoBackTest'


##Add a constraint here, cffex instruments can not go along with others 
##Their trading rules, timestamp are all different
class CFFEXBreak(dbBase.DB_BASE):
    
    def __init__(self):
        db_name,table_name = 'config','break_cffex'
        super(CFFEXBreak,self).__init__(db_name)
        
        self.table_struct = Table(table_name,self.meta,
             Column('type',String(20),primary_key = True),
             Column('spots_perday',Integer),
             Column('spots_interval',Integer),
             Column('break_spots',String(100)),
             Column('break_millis',String(100)),
            )
        
        self.commodity_info_obj = self.quick_map(self.table_struct)
        
    def set_value(self,type,spots_perday,spots_interval,break_spots,break_millis):
        self.insert_listlike(self.commodity_info_obj,(type,spots_perday,spots_interval,break_spots,break_millis))
    
    def get_value(self,stype = 'tick'):
        ss = self.get_session()
        records = ss.query(self.commodity_info_obj).filter_by(type = stype).first()
        if records:
            ret = records.type,records.spots_perday,records.spots_interval,records.break_spots,records.break_millis
        ss.close()
        return ret
   
class CommodityInfo(dbBase.DB_BASE):
    
    def __init__(self):
        db_name,table_name = 'config','commodity_info'
        super(CommodityInfo,self).__init__(db_name)
        
        self.table_struct = Table(table_name,self.meta,
             Column('ID',Integer,primary_key = True,autoincrement = False),
             Column('marketID',Integer),
             Column('ticker',String(20)),
             Column('startMilli',Integer),
             Column('endMilli',Integer),
             Column('sampleFreq',Integer),
             Column('startDate',Integer),
             Column('endDate',Integer),
             Column('point',Integer),
             Column('exchangeRate',Float),
             Column('adjust',Float),
             Column('adjust2',Float),
             Column('sector',Integer),
             Column('etfID',Integer),
             Column('sellShort',Integer),
             Column('lunchBreakStartMilli',Integer),
             Column('lunchBreakEndMilli',Integer),
             Column('slippage',Float),
             Column('positionCap',Integer),
             Column('off2price',Float),
             Column('exchangeMargin',Float),
             Column('brokerMargin',Float),
             Column('oneUnit',Float),
             Column('basicFee',Float),
             Column('extraFee',Float),
             Column('singleSideFee',Integer),
             Column('feeType',Integer),
             Column('limit1',Float),
             Column('limit2',Float),
             Column('limit3',Float)
            )
        
        self.commodity_info_obj = self.quick_map(self.table_struct)
        
class IndicatorIDs(object):
    
    def __init__(self):
        
        ##------------------------------------------------------##
        self.trade_type_map = {}
        self.trade_type_map['LONG_OPEN'] = 8
        self.trade_type_map['SHORT_OPEN'] = 7
        self.trade_type_map['BOTH_OPEN'] = 1
        self.trade_type_map['LONG_SWITCH'] = 3
        self.trade_type_map['SHORT_SWITCH'] = 2
        self.trade_type_map['LONG_CLOSE'] = 4
        self.trade_type_map['SHORT_CLOSE'] = 5
        self.trade_type_map['BOTH_CLOSE'] = 6
        self.trade_type_map['LOCK'] = 9
        self.trade_type_map['NAN'] = 0
        
        self.trade_type_map = self.dict_process(self.trade_type_map)
        
        ##------------------------------------------------------##
        self.trend_type_map = {}
        self.trend_type_map['BUY'] = 1
        self.trend_type_map['SELL'] = -1
        self.trend_type_map['NAN'] = 0
        
        self.trend_type_map = self.dict_process(self.trend_type_map)
        
        ##------------------------------------------------------##
        self.tick_map = {}
        self.tick_map['LAST_PRICE'] = 0
        self.tick_map['VOLUME'] =  1
        self.tick_map['PRE_CLOSE_PRICE'] = 2

        self.tick_map['ASK_PRICE']  = 4
        self.tick_map['ASK_VOLUME'] =  5
        self.tick_map['BID_PRICE'] = 6
        self.tick_map['BID_VOLUME'] = 7

        self.tick_map['LOWER_LIMIT_PRICE'] =  10
        self.tick_map['UPPER_LIMIT_PRICE'] =  11
        self.tick_map['CLOSE_PRICE'] =  12
        self.tick_map['OPEN_INTEREST'] =  13
        self.tick_map['TURNOVER'] =  16
        self.tick_map['INSTRUMENT_ID'] = 17
        self.tick_map['CLOSE_POSITION'] = 18
        self.tick_map['OPEN_POSITION'] = 19
        self.tick_map['INC_POSITION'] = 20
        self.tick_map['TRADE_TYPE'] = 21
        self.tick_map['TICK_TREND'] = 22
        self.tick_map['MID_PRICE'] = 40
        self.tick_map['WEIGHTED_PRICE'] = 41
        self.tick_map['LOG_PI'] = 43
        
        self.tick_map = self.dict_process(self.tick_map)
        
        ##------------------------------------------------------##
        self.other_map = {}
        self.other_map['OPEN'] = 100
        self.other_map['HIGH'] = 101
        self.other_map['LOW'] = 102
        self.other_map['CLOSE'] = 103
        self.other_map['INTERVAL_VOLUME'] = 104
        self.other_map['OPEN_INTEREST'] = 105
        self.other_map['BID_PRICE'] = 106
        self.other_map['ASK_PRICE'] = 107
        self.other_map['BID_VOLUME'] = 108
        self.other_map['ASK_VOLUME'] = 109
        self.other_map['TURNOVER'] = 110

        self.other_map['OPEN_POSITION'] = 111
        self.other_map['CLOSE_POSITION'] = 112
        self.other_map['CUL_VOLUME'] = 113

        self.other_map['MA5'] = 120
        self.other_map['MA10'] = 121
        self.other_map['MA20'] = 122
        self.other_map['TR'] = 123
        self.other_map['ATR'] = 124

        self.other_map['UM'] = 137
        self.other_map['DM'] = 138
        self.other_map['DEMEAN_R'] = 139
        self.other_map['VWAP'] = 142
        self.other_map['TREND'] = 150
        
        self.other_map = self.dict_process(self.other_map)
        
    def dict_process(self,indict):
        newdict = copy.deepcopy(indict)
        for k,v in indict.iteritems():
            newdict[str.lower(k)] = v
            newdict[''.join(k.split('_'))] = v
            newdict[str.lower(''.join(k.split('_')))] = v
        return newdict

def test_ids():
    ##check IDs
    indIDs = IndicatorIDs()
    print indIDs.other_map
    print indIDs.tick_map
    print indIDs.trade_type_map
    print indIDs.trend_type_map
    
def test_info_struct():
    ##check commInfo
    comInfo = CommodityInfo()
    print comInfo.get_column_names(comInfo.commodity_info_obj)
    inVals = '''110010001    11    IF0001    33300000    54900000    500    20130108    20130711    300    1    -9999    -9999    11    -9999    1    41400000    41400000    0    10    -9999    0.12    0.15    0.2    0.000025    0    1    0    0.1    0.1    0.1'''.split()
    comInfo.insert_listlike(comInfo.commodity_info_obj,inVals)

def test_break_info():
    ##set break info
    breakinfo = CFFEXBreak()
    breakinfo.set_value('tick', 32402, 500, '0,16203', '33300000,46800000')
    print breakinfo.get_value('tick')
    
if __name__ == '__main__':
    test_break_info()
    