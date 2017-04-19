import os
import redis_block_config
import argparse
import copy
from redis_block_config import Dates
from misc import dict_to_lower

basic_indicators = ['LastPrice','TradeVolume','BidPrice','BidVolume','AskPrice','AskVolume','OpenInterest']

def verify(arg_dict):
    
    if arg_dict['start_date'] < 20100101 or arg_dict['start_date'] > 20170101:
        return -1
    
    if arg_dict['level'] != 'tick' and arg_dict['level'] != '1min' and arg_dict['level'] != '5min':
        return -2
    
    if arg_dict['instruments'] is None:
        return -3
    
    if len(arg_dict['instruments']) < 1:
        return -3 
    
    dates = Dates()
    if dates.get_first_bigger_than( arg_dict['start_date'] ) is None:
        return -4
    if dates.get_first_less_than( arg_dict['end_date'] ) is None:
        return -4
    
    return 0
    
def get_indicator_list(input_inds):
    inds = set(copy.deepcopy(basic_indicators))
    if input_inds is not None:
        for i in input_inds.split(','):
            inds.add(i)
        return list(inds)
    else:
        return list(inds)

def get_instrument_list(input_inss):
    if input_inss is not None:
        return map(lambda x: str.lower(x),input_inss.split(','))
    else:
        return []
    
def check_or_add_db(arg_dict):
    arg_dict = dict_to_lower(arg_dict)
    db_api = redis_block_config.block_config_api()
    re = db_api.belong_to(arg_dict)
    if re != -1:
        return re
    elif re == -1:
        exist_ids = set(db_api.list_ids()) 
        if len(exist_ids) > 0:
            new_id = [ i for i in range(max(exist_ids) + 2) if i not in exist_ids][0]
        else:
            new_id = 0
        db_api.set_id(new_id,arg_dict)
        return new_id
    
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-start_date',dest = 'start_date',nargs = '?',type = int)
    parser.add_argument('-end_date',dest = 'end_date',nargs = '?',type = int)
    parser.add_argument('-level',dest = 'level',nargs = '?',type = str,default = 'tick')
    parser.add_argument('-type',dest = 'type',nargs = '?',type = str,default = 'future')
    parser.add_argument('-adjust',dest = 'adjust',nargs = '?',type = int,default = 0)
    parser.add_argument('-indicators',dest = 'indicators',nargs = '?',type = str)
    parser.add_argument('-instruments',dest = 'instruments',nargs = '?',type = str)
    args = parser.parse_args()
    arg_dict = vars(args)

    if verify(arg_dict) < 0:
        print 'input format error'
        exit(-1)

    adjust = int(arg_dict['adjust'])
    arg_dict['indicators'] = get_indicator_list(arg_dict['indicators'])
    arg_dict['instruments'] = get_instrument_list(arg_dict['instruments'])
    arg_dict = dict_to_lower(arg_dict)
    print 'arg_dict = ',arg_dict
    
    id = check_or_add_db(arg_dict)
    print 'dispathed id = ',id
    