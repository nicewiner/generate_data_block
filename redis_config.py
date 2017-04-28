import json
import redis
import collections

import sys
sys.path.append("..")
from future_mysql import dbBase

from sqlalchemy import Column, Integer
from sqlalchemy import Table
from config_vars import  Dates
from misc import dict_to_lower,unicode2str

class ipc_db_api(object):
    
    def __init__(self,db_addr = '127.0.0.1',db_port = 6379,db_name = 2):
        self.iredis = redis.Redis(host = db_addr ,
                                  port = db_port,
                                  db   = db_name)
    
    def get_key(self,date,username,pid,requestID,funcName):
        return ':'.join((date,username,pid,requestID,funcName))
    
    def set_value(self,key,**value):
        json_str = json.dumps(value)
        self.iredis.set(key,json_str)
    
    def get_value(self,key):
        if self.iredis.exists(key):
            value = self.iredis.get(key)
        else:
            return None
        pydict = json.loads(value)
        return unicode2str(pydict)
            
class block_config_api(object):
    
    def __init__(self,db_addr = '127.0.0.1',db_port = 6379,db_name = 1):
        self.iredis = redis.Redis(host = db_addr ,
                                  port = db_port,
                                  db   = db_name)
        self.prefix = 'block_config'
        self.is_trading_date = False
        
        self.dates = Dates()
        
    def id2key(self,_key):
        return '_'.join((self.prefix,str(_key)))
    
    def key2id(self,_key):
        return int(_key.split('_')[-1])
    
    #make sure the date in redis is really trading date
    def set_id(self,id,pydict):
               
        if not self.is_trading_date:
            pydict['start_date'] = self.dates.get_first_bigger_than(pydict['start_date'])
            pydict['end_date'] = self.dates.get_first_less_than(pydict['end_date'])
            self.is_trading_date = True
        
        pydict = dict_to_lower(pydict)
        
        json_str = json.dumps(pydict)
        key = self.id2key(id)
        self.iredis.set(key,json_str)

    def get_id(self,id):
        key = self.id2key(id)
        if self.iredis.exists(key):
            value = self.iredis.get(key)
        else:
            return None
        pydict = json.loads(value)
        return unicode2str(pydict)
    
    def erase_id(self,id):
        key = self.id2key(id)
        if self.iredis.exists(key):
            self.iredis.delete(key)
    
    def list_ids(self):
        keys = self.iredis.keys(self.prefix + '*')
        return map(lambda x: int(x.split('_')[-1]),keys)

    def belong_to(self,pydict):
        
        if not self.is_trading_date:
            pydict['start_date'] = self.dates.get_first_bigger_than(pydict['start_date'])
            pydict['end_date'] = self.dates.get_first_less_than(pydict['end_date'])
            self.is_trading_date = True
            
        ids = self.list_ids()
        for id in ids:
            db_pydict = self.get_id(id)
            equal = True
            for ikey,ival in pydict.iteritems():
                vval = db_pydict[ikey]
                if isinstance(vval,collections.Iterable):
                    re = set(ival).issubset( set(vval) )
                    if not re :
                        equal = False
                        break
                else:
                    if ikey == 'start_date':
                        if ival < vval:
                            equal = False
                            break
                    elif ikey == 'end_date':
                        if ival > vval:
                            equal = False
                            break
                    else:
                        if ival != vval:
                            equal = False
                            break
            if equal:
                return id
        return -1
            
if __name__ == '__main__':
    
    ##Test db
    '''try set'''
    text = {'level':'tick','type':'future','adjust':0,'start_date':20140101,'end_date':20150102,'indicators':["LastPrice","Volume"],'instruments':['if0001','if0002']}
    text = dict_to_lower(text)
    
    dbapi = block_config_api()
    dbapi.set_id(0,text)
    
    '''try get'''
    pydict = dbapi.get_id(0)
    print pydict
    
    '''try list'''
    ids = dbapi.list_ids()
    print ids
    
    '''try delete'''
    for id in ids:
        dbapi.erase_id(id)
        
    ##test Dates
    dates = Dates()
    print 'test first bigger than'
    print dates.get_first_bigger_than(20090101)
    print dates.get_first_bigger_than(20190101)
    
    print 'test first less than'
    print dates.get_first_less_than(20090101)
    print dates.get_first_less_than(20190101)
        