
import os
import ConfigParser
from config_vars import CFFEXBreak, CommodityInfo, IndicatorIDs, GlobalVar, Ticker
import collections
from redis_block_config import block_config_api
from misc import dict_to_lower
from redis_block_config import Dates

g_v = GlobalVar()
num_of_trading_days = 0
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
    global num_of_trading_days
    num_of_trading_days = len(records)
    print 'daylen = ',len(records)
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
    global basic_dir
    pair_path = os.path.join(basic_path,'instrumentsPair.list')
    list_path = os.path.join(basic_path,'insList.list')
    print pair_path,'\n',list_path
    
    ticker = Ticker()
    
    with open(pair_path,'w+') as pair_out:
        with open(list_path,'w+') as list_out:
            for iticker in pydict['instruments']:
                ins_id = ticker.get_id(iticker)
                if ins_id:
                    pair_out.write(iticker + ' ' + str(ins_id))
                    pair_out.write('\n')
                    
                    list_out.write(str(ins_id))
                    list_out.write('\n')
    
def create_shm_alloc_ini(dispatch_id,pydict):
    
    global basic_path
    tar_path = os.path.join(basic_path,'ShmAlloc.ini')
    print tar_path
    
    breakinfo = CFFEXBreak()
    type,spots_perday,spots_interval,break_spots,break_millis = breakinfo.get_value(pydict['level'])
    
    ##part 1
    global num_of_trading_days
    header_tag = 'HEADER_BLOCK'
    spots_count = num_of_trading_days * spots_perday + spots_perday #extra day
    spots_interval = spots_interval
    spots_count_perday = spots_perday
    begin_date = pydict['start_date']
    history_data_duration = num_of_trading_days
    begin_millisec_in_day = 33300000
    stream_type = 4867
    indicators_list_file = os.path.join(basic_path,'indList.list')
    sub_instruments_list_file = os.path.join(basic_path,'insList.list')
    last_available_spot = -1
    
    cinfo = CommodityInfo()
    ticker = Ticker()
    first_ticker = pydict['instruments'][0]
    ins_id = ticker.get_id(first_ticker)
    objs = cinfo.query_obj(cinfo.commodity_info_obj,ID = ins_id)
    if isinstance(objs,collections.Iterable):
        begin_millisec_in_day = objs[0].startMilli
    
    scp = ConfigParser.SafeConfigParser()
    scp.add_section(header_tag)
    scp.set(header_tag, 'spots_count', str(spots_count) )
    scp.set(header_tag, 'spots_interval', str(spots_interval) )
    scp.set(header_tag, 'spots_count_perday', str(spots_count_perday) )
    scp.set(header_tag, 'begin_date', str(begin_date) )
    scp.set(header_tag, 'history_data_duration', str(history_data_duration) )
    scp.set(header_tag, 'begin_millisec_in_day', str(begin_millisec_in_day) )
    scp.set(header_tag, 'stream_type', str(stream_type) )
    scp.set(header_tag, 'indicators_list_file', indicators_list_file)
    scp.set(header_tag, 'sub_instruments_list_file', sub_instruments_list_file)
    scp.set(header_tag, 'last_available_spot', str(last_available_spot) )

    ##part 2
    shm_tag = 'SHM'
    limit_size = 0
    indIDs = IndicatorIDs()
    idmap = indIDs.tick_map if pydict['level'] == 'tick' else indIDs.other_map
    bytemap = indIDs.tick_byte4 if pydict['level'] == 'tick' else indIDs.other_byte4
    for ind_name in pydict['indicators']:
        ind_id = idmap[ind_name]
        if ind_id:
            data_size = 8 if idmap[ind_name] not in bytemap else 4
            limit_size += data_size * spots_count
    ipc_key = '0x0f0f%04d' % (dispatch_id)
    auth = 438
    info_size = 10240000
    info_key = '0x0e0e%04d' % (dispatch_id)   
    info_auth = 438        
    
    scp.add_section(shm_tag)
    scp.set(shm_tag, 'limit_size', str(limit_size) )
    scp.set(shm_tag, 'ipc_key', ipc_key )
    scp.set(shm_tag, 'auth', str(auth) )
    
    scp.set(shm_tag, 'info_size', str(info_size) )
    scp.set(shm_tag, 'info_key', info_key )
    scp.set(shm_tag, 'info_auth', str(info_auth) )       
    
    ##part3
    info_tag = 'INFO_HEADER'
    trading_date_list = os.path.join(basic_path,'tradingDay.list')
    commodity_info_list = os.path.join(basic_path,'CommodityInfo.list')
    instruments_pair_list = os.path.join(basic_path,'instrumentsPair.list')
    indicator_pair_list = os.path.join(basic_path,'indicatorsPair.list')
    break_info_ini = os.path.join(basic_path,'break.ini')
    scp.add_section(info_tag)
    scp.set(info_tag, 'trading_date_list', trading_date_list)
    scp.set(info_tag, 'commodity_info_list', commodity_info_list)
    scp.set(info_tag, 'instruments_pair_list', instruments_pair_list)
    scp.set(info_tag, 'indicator_pair_list', indicator_pair_list)
    scp.set(info_tag, 'break_info_ini', break_info_ini)
    
    with open(tar_path,'w+') as fout:
        scp.write(fout)

def startup_shm():
    pass
           
if __name__ == '__main__':
    
    dispatch_id = 0
    dbapi = block_config_api()
    pydict = dbapi.get_id(dispatch_id)
    print pydict
    
    set_basic_path(dispatch_id)
    generate_break(pydict)
    generate_commodity_info(pydict)
    create_trading_day_list(pydict)
    create_ind_and_pair(pydict)
    create_ins_and_pair(pydict)
    create_shm_alloc_ini(dispatch_id,pydict)
    