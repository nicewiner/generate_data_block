import pandas as pd
import os
import ConfigParser
from config_vars import CFFEXBreak, CommodityInfo, IndicatorIDs, GlobalVar, Ticker
import collections
from redis_block_config import block_config_api
from misc import dict_to_lower
from redis_block_config import Dates

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
        fout.write('\n')
        for ins in pydict['instruments']:
            id = ticker.get_id(ins)
            objs = cinfo.query_obj(cinfo.commodity_info_obj,ID = id)
            if isinstance(objs,collections.Iterable):
                obj = objs[0]
            else:
                continue
            obj.startDate = pydict['start_date']
            obj.endDate   = pydict['end_date']
            sout = '\t'.join(map(lambda x:str(getattr(obj,x)), cinfo.get_column_names(cinfo.commodity_info_obj)))
            fout.write(sout)
            fout.write('\n')

def create_trading_day_list(pydict):
    from sqlalchemy import and_
    dates = Dates()
    ss = dates.get_session()
    records = ss.query(dates.trading_day_obj).filter(and_(dates.trading_day_obj.date >= pydict['start_date'],dates.trading_day_obj.date <= pydict['end_date'] )).all()
    ss.close()
    global basic_path
    tar_path = os.path.join(basic_path,'tradingDay.list')
    print tar_path
    with open(tar_path,'w+') as fout:
        for day in records:
            fout.write(str(day.date))
            fout.write('\n')
    return len(records)
        
def create_ind_and_pair(pydict):
    from ind_cal import IndCal
    indIDs = IndicatorIDs()
    idmap = indIDs.tick_map if pydict['level'] == 'tick' else indIDs.other_map
    bytemap = indIDs.tick_byte4 if pydict['level'] == 'tick' else indIDs.other_byte4
    global basic_dir
    pair_path = os.path.join(basic_path,'indicatorsPair.list')
    list_path = os.path.join(basic_path,'indList.list.list')
    print pair_path,'\n',list_path
    with open(pair_path,'w+') as pair_out:
        with open(list_path,'w+') as list_out:
            for ind_name in pydict['indicators']:
                if ind_name in idmap.keys():
                    pair_out.write(ind_name + ' ' + str(idmap[ind_name]))
                    pair_out.write('\n')
                    data_size = 8 if idmap[ind_name] not in bytemap else 4
                    list_out.write(str(idmap[ind_name]) + ' ' + str(data_size))
                    list_out.write('\n')
                else:
                    print 'invalid indicator ',ind_name

def create_ins_and_pair(pydict):
    pass

if __name__ == '__main__':
    
    dispatch_id = 0
    dbapi = block_config_api()
    pydict = dbapi.get_id(dispatch_id)
    print pydict
    
    set_basic_path(dispatch_id)
    generate_break(pydict)
    generate_commodity_info(pydict)
    print 'daylen = ',create_trading_day_list(pydict)
    create_ind_and_pair(pydict)

