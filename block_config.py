import json
import redis

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
    
    def set_id(self,id,json_pydict):
        json_str = json.dumps(json_pydict)
        key = self.id2key(id)
        self.iredis.set(key,json_str)

    def get_id(self,id):
        key = self.id2key(id)
        if self.iredis.exists(key):
            value = self.iredis.get(key)
        json_pydict = json.loads(value)
        return json_pydict
    
    def erase_id(self,id):
        key = self.id2key(id)
        if self.iredis.exists(key):
            self.iredis.delete(key)
    
    def list_ids(self):
        keys = self.iredis.keys(self.prefix + '*')
        return map(lambda x: int(x.split('_')[-1]),keys)
    
if __name__ == '__main__':
    
    '''try set'''
    text = {'start_date':20160101,'end_date':20160102,'indicators':["lastPrice","Volume"],'instruments':['if0001','if0002']}
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
        