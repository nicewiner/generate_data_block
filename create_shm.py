
import os
import ConfigParser
import collections
from config_vars import CFFEXBreak, CommodityInfo, IndicatorIDs, GlobalVar, Ticker
from redis_block_config import block_config_api, Dates
 
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
            _id = ticker.get_id(ins)
            objs = cinfo.query_obj(cinfo.commodity_info_obj,ID = _id)
            if isinstance(objs,collections.Iterable):
                obj = objs[0]
            else:
                continue
            obj.startDate = pydict['start_date']
            obj.endDate   = pydict['end_date']
            obj.basicFee  = '%f' %(obj.basicFee)
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
    list_path = os.path.join(basic_path,'indList.list')
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
        if ind_id is not None:
            data_size = 8 if idmap[ind_name] not in bytemap else 4
            limit_size += data_size
    limit_size *= len(pydict['instruments']) * spots_count
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
        
    #generate config.ini for data_loader
    tar_path = os.path.join(basic_path,'config.ini')
    print tar_path
    scp2 = ConfigParser.SafeConfigParser()
    tag = 'HEADER_BLOCK'
    scp2.add_section(tag)
    scp2.set(tag,'SPOTS_COUNT',str(num_of_trading_days * spots_perday))
    scp2.set(tag,'SPOTS_INTERVAL',str(spots_interval))
    scp2.set(tag,'SPOTS_COUNT_PERDAY',str(spots_count_perday))
    scp2.set(tag,'BEGIN_DATE',str(begin_date))
    scp2.set(tag,'HISTORY_DATA_DURATION',str(history_data_duration))
    scp2.set(tag,'BEGIN_MILLISEC_IN_DAY',str(begin_millisec_in_day))
    with open(tar_path,'w+') as fout:
        scp2.write(fout)
    
def startup_shm(dispatch_id):
    from misc import get_active_IPC
    active_ipcs = get_active_IPC()
    
    ipc_key = '0x0f0f%04d' % (dispatch_id)
    info_key = '0x0e0e%04d' % (dispatch_id)
    if ( ipc_key in active_ipcs ) or (info_key in active_ipcs):
        print 'error, shm exist'
        return -1
    
    global basic_path
    DataServer = r'/quant/bin/DataServer'
    cmd = "{0} -c -i -f {1}".format(DataServer,os.path.join(basic_path,'ShmAlloc.ini'))
    print cmd
    os.system(cmd)
    
    active_ipcs = get_active_IPC()
    if ( ipc_key not in active_ipcs ) or (info_key not in active_ipcs):
        print 'error, can not create shm'
        return -2
    else:
        return 0

def load_from_bin(load_path):
    DataLoader = r'/quant/bin/DataLoader'
    
    scp = ConfigParser.SafeConfigParser()
    scp.read(os.path.join(load_path,'ShmAlloc.ini' ))
    ipc_key = scp.get('SHM','IPC_KEY') 
    
    cmd = "{0} -w {1} -i {2}".format(DataLoader, load_path, ipc_key)  
    os.system(cmd)
    
def load_data_tick(pydict):
    
    global basic_path
    if os.path.exists(os.path.join(basic_path,'data.bin')):
        load_from_bin(basic_path)
        return 0
    
    import sys
    sys.path.append('..')
    
    from future_mysql.data_import import cffex_if
    from future_mysql.trading_day_list import futureOrder
    from config_vars import Dates
    import ShmPython
    import pandas as pd
    import numpy as np
    
    tar_path = os.path.join(basic_path,'tradingDay.list')
    with open(tar_path,'r') as fin:
        trading_day_list = [ int( i.strip()) for i in fin]
    
    config_path = os.path.join(basic_path,'ShmAlloc.ini')
    scp = ConfigParser.SafeConfigParser()
    scp.read(config_path)
    spots_perday = int(scp.get('HEADER_BLOCK','spots_count_perday'))
    ipc_key = scp.get('SHM','ipc_key')
    print spots_perday,ipc_key
    shm_api = ShmPython.Shm(ipc_key)
    
    if not shm_api.isConnectGood():
        print 'no such shm'
        return -1
    
    ticker = Ticker()
    
    market_ids = set([ ticker.get_market_id(ins_name) for ins_name in pydict['instruments'] ])
    marketid2inss = {}
    for mid in market_ids:
        marketid2inss[mid] = [ ins_name for ins_name in pydict['instruments'] if ticker.get_market_id(ins_name) == mid]
    print marketid2inss
    print market_ids

    indAPI = IndicatorIDs()
    
    for market_id in market_ids: 
        print market_id
        ins_names = marketid2inss[market_id]
        db_name = ticker.get_dbname(ins_names[0])
        for nthday,iday in enumerate(trading_day_list):
            print iday,db_name
            if db_name == 'cffex_if':
                data_table = cffex_if(db_name,iday)
                data_order = futureOrder('if')
                table_name = '.'.join((db_name,str(iday)))
            
            if not data_table.check_table_exist():
                print 'table does not exist!'
                continue
            
            order_record = data_order.query_obj(data_order.future_order_struct,date = iday)[0]
            ticker_ids = [ getattr(order_record, ins_name) for ins_name in ins_names ] 
            conditions = 'OR'.join([" id = '{0}' ".format(ticker_id) for ticker_id in ticker_ids])
            sql = 'select * from {0} where {1} order by spot;'.format(table_name,conditions)
            
            df = pd.read_sql(sql,data_table.engine,index_col = ['spot'])
            df.columns = map(lambda x:str.lower(str(x)),df.columns)
            for nth_ins,real_ticker in enumerate(ticker_ids):
                
                ins_name = ins_names[nth_ins]
#                 print ins_name
                ins_id   = ticker.get_id(ins_name)
                ins_index = shm_api.id2index_ins(ins_id)
#                 print 'ins_name = {0}, ins_id = {1}, ins_index = {2}'.format(ins_name,ins_id,ins_index)
                sub_df = df.loc[df['id'] == real_ticker]                
                for ind_name in pydict['indicators']:
                    ind_id = indAPI.tick_map[ind_name]
                    ind_index = shm_api.id2index_ind(ind_id)
                    data_size = 4 if ind_id in indAPI.tick_byte4 else 8
                    begin_spots = nthday * spots_perday
#                     print 'ind_name = {0}, ind_id = {1}, ind_index = {2}, ind_size = {3}'.format(ind_name,ind_id,ind_index,data_size)
                    if data_size == 4:
                        values = sub_df.ix[:,ind_name].apply(np.int).values
                        shm_api.dumpIntDataList(values,ind_index,ins_index,begin_spots,begin_spots + spots_perday)
                    else:
                        values = sub_df.ix[:,ind_name].values
                        shm_api.dumpDoubleDataList(values,ind_index,ins_index,begin_spots,begin_spots + spots_perday)
                       
    if not os.path.exists(os.path.join(basic_path,'data.bin')):
        export2bin(shm_api)
    
    return 0

def export2bin(shm_api):
    global basic_path
    if not shm_api.isConnectGood():
        print 'no such shm'
        return -1
    day_length = shm_api.getHeader().getHistoryDataDuration()
    output_dir = os.path.join(basic_path,'export')
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    shm_api.export_shm(output_dir, day_length, False)
    bin_name = filter(lambda x:str.endswith(x,'bin'),os.listdir(output_dir))[-1]
    print os.path.join(output_dir,bin_name),os.path.join(basic_path,'data.bin')
    os.system('mv {0} {1}'.format(os.path.join(output_dir,bin_name),os.path.join(basic_path,'data.bin')))
        
class ShmCreator(object):
    
    def __init__(self,dispatch_id):
        self.id = dispatch_id
        set_basic_path(dispatch_id)
        global basic_path
        self.basic_path = basic_path
        
    def generate(self,force_reload = False):
        
        from misc import ipc_exist
        dbapi = block_config_api()
        pydict = dbapi.get_id(self.id)
        if os.path.exists(self.basic_path):
            print 'config files already created'
            pass
        else:
            generate_break(pydict)
            generate_commodity_info(pydict)
            create_trading_day_list(pydict)
            create_ind_and_pair(pydict)
            create_ins_and_pair(pydict)
            create_shm_alloc_ini(self.id,pydict)
            
        config_path = os.path.join(self.basic_path,'ShmAlloc.ini')
        scp = ConfigParser.SafeConfigParser()
        scp.read(config_path)
        ipc_key = scp.get('SHM','ipc_key')
        
        if not ipc_exist(ipc_key):
            startup_shm(self.id)
            load_data_tick(pydict)
        else:
            if force_reload:
                load_data_tick(pydict)
    
    #to do: 1.check whether there is enough memory space 2.delete extra ipckey        
    def config_os_memory(self):
        pass
            
if __name__ == '__main__':
    
    dispatch_id = 0
    shm_creator = ShmCreator(dispatch_id)
    shm_creator.generate()
    
    #for debug
#     dispatch_id = 0
#     dbapi = block_config_api()
#     pydict = dbapi.get_id(dispatch_id)
#     set_basic_path(dispatch_id)
#     generate_commodity_info(pydict)
#     create_trading_day_list(pydict)
#     create_shm_alloc_ini(dispatch_id,pydict)
#     startup_shm(dispatch_id)
#     load_data_tick(pydict)

