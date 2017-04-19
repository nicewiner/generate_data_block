import pandas as pd
import os
import ConfigParser
from config_vars import CFFEXBreak, CommodityInfo, IndicatorIDs, GlobalVar, Ticker

import sys
sys.path.append("..")
from future_mysql.time2point import DayMode

from misc import dict_to_lower

g_v = GlobalVar()
basic_path = ''

def set_basic_path(dispatch_id):
    global basic_path
    basic_path = os.path.join(g_v.basic_path,str(dispatch_id) )
    if not os.path.exists(basic_path):
        os.mkdir(basic_path)

def generate_break(pydict):
    tag1,tag2,tag3 = 'general','spot','millisec'
    breakinfo = CFFEXBreak()
    type,spots_perday,spots_interval,break_spots,break_millis = breakinfo.get_value(pydict['level'])
    print type,spots_perday,spots_interval,break_spots,break_millis
    
    scp = ConfigParser.SafeConfigParser()
    scp.add_section(tag1)
    scp.set(tag1,'spots_perday',str(spots_perday) )
    scp.set(tag1,'spots_interval',str(spots_interval) )
    scp.add_section(tag2)
    scp.set(tag2,'break_spots',break_spots)
    scp.add_section(tag3)
    scp.set(tag3,'break_millis',break_millis)
    
    global basic_path
    tar_path = os.path.join(basic_path,'break.ini')
    print tar_path
    with open(tar_path,'w+') as break_out:
        scp.write(break_out)
    
def generate_commodity_info(pydict):
    global basic_path
    tar_path = os.path.join(basic_path,'CommodityInfo.list')
    print tar_path    
    
    cinfo = CommodityInfo()
    colnames = cinfo.get_column_names(cinfo.commodity_info_obj)
    
    ticker = Ticker()
    
    with open(tar_path,'w+') as fout:
        fout.write('\t'.join(colnames))
        for ins in pydict['instruments']:
            id = ticker.get_id(ins)
            print id
            obj = cinfo.query_obj(cinfo.commodity_info_obj,ID = id)[0]
            print map(lambda x:getattr(obj,x), cinfo.get_column_names(cinfo.commodity_info_obj))
    
if __name__ == '__main__':
    
    pydict = {'level':'tick','type':'future','adjust':0,'start_date':20130101,'end_date':20140102,'indicators':['LastPrice','TradeVolume','BidPrice1','BidVolume1','AskPrice1','AskVolume1','OpenInterest'],'instruments':['if0001','if0002']}
    pydict = dict_to_lower(pydict)
    dispatch_id = 0
    
    set_basic_path(dispatch_id)
    generate_break(pydict)
    generate_commodity_info(pydict)

