import json
import redis
import collections

class block_config_api(object):
    
    def __init__(self,db_addr = '127.0.0.1',db_port = 6379,db_name = 1):
        self.iredis = redis.Redis(host = db_addr ,
                                  port = db_port,
                                  db   = db_name)
        self.prefix = 'block_config'
        
    def id2key(self,_key):
        return '_'.join((self.prefix,str(_key)))
    
    def key2id(self,_key):
        return int(_key.split('_')[-1])
    
    def set_id(self,id,pydict):
        json_str = json.dumps(pydict)
        key = self.id2key(id)
        self.iredis.set(key,json_str)

    def get_id(self,id):
        key = self.id2key(id)
        if self.iredis.exists(key):
            value = self.iredis.get(key)
        pydict = json.loads(value)
        return pydict
    
    def erase_id(self,id):
        key = self.id2key(id)
        if self.iredis.exists(key):
            self.iredis.delete(key)
    
    def list_ids(self):
        keys = self.iredis.keys(self.prefix + '*')
        return map(lambda x: int(x.split('_')[-1]),keys)

    def cmp(self,pydict):
        ids = self.list_ids()
        for id in ids:
            db_pydict = self.get_id(id)
            equal = True
            for ikey,ival in pydict.iteritems():
                vval = db_pydict[ikey]
                if isinstance(vval,collections.Iterable):
                    re = cmp(sorted(ival),sorted(vval))
                    if re != 0:
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
    
    '''try set'''
    text = {'level':'tick','type':'future','adjust':0,'start_date':20160101,'end_date':20160102,'indicators':["lastPrice","Volume"],'instruments':['if0001','if0002']}
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
        