import os
import block_config
import argparse

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
    
def get_indicator_list(input_inds):
    if input_inds is not None:
        return basic_indicators.extend(input_inds.split(','))
    else:
        return basic_indicators

def get_instrument_list(input_inss):
    if input_inss is not None:
        return map(lambda x: str.lower(x),input_inss.split(','))
    else:
        return []
    
def check_db(arg_dict):
    db_api = block_config.block_config_api()
    re = db_api.cmp(arg_dict)
    if re != -1:
        return re
    elif re == -1:
        exist_ids = set(db_api.list_ids()) 
        new_id = set(range(exist_ids[-1]+1)) - exist_ids
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
    print arg_dict

    if verify(arg_dict) is False:
        print 'input format error'
        exit()

    adjust = int(arg_dict['adjust'])
    arg_dict['indicators'] = get_indicator_list(arg_dict['indicators'])
    arg_dict['instruments'] = get_instrument_list(arg_dict['instruments'])
    print arg_dict

    