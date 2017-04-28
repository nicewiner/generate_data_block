import zmq
import threading
import functools
import os
from create_shm import create_shm_and_load_data
from misc import get_today

root_path = r'/home/xudi/autoBackTest'

class RPC_CLIENT(object):
    
    def __init__(self,username):
        self.today = get_today()
        self.username = username
        
    def connect(self,addr):
        self.ctx  = zmq.Context(1)
        self.socket = self.ctx.socket(zmq.PUSH)
        self.socket.connect(addr)
        
    def create_cmd(self,jobid,jobtype):
        key = '{0}:{1}:{2}:{3}'.format(self.today,self.username,jobid,jobtype)
        hashv = hash(key)
        return {'date':self.today,'username':self.username,'jobid':jobid,'jobtype':jobtype,'hashv':hashv}
    
    def send_cmd(self,cmd):
        self.socket.send_json(cmd)
        
class RPC_SERVER(threading.Thread):
    
    def __init__(self,callbacks):
        super(RPC_SERVER, self).__init__()
        self.callbacks = callbacks
        
    def connect(self,addr):
        self.ctx  = zmq.Context(1)
        self.socket = self.ctx.socket(zmq.PULL)
        self.socket.connect(addr)
    
    def verify_cmd(self,cmd):
        try:
            today,username,jobid,jobtype,hashv = cmd['date'],cmd['username'],cmd['jobid'],cmd['jobtype'],cmd['hashv']
            if str(hash(':'.join((today,username,jobid,jobtype)))) != hashv:
                return None
            else:
                return cmd
        except:
            return None
       
    def recv_cmd(self):
        while True:
            cmd = self.socket.recv_json()
            if cmd is not None:
                ret = self.callbacks[cmd['jobtype']]()
                
                
    def run(self):
        self.recv_cmd()   
    
class IpcProxy(threading.Thread):
    
    def __init__(self,direction = 1):
        super(IpcProxy, self).__init__()
        if direction == 1:
            self.stream_direction = 'employee'
        else:
            self.stream_direction = 'worker'
        self.frontend_addr = r"ipc://~/tmp/{0}_frontend".format(self.stream_direction)
        self.backend_addr  = r"ipc://~/tmp/{0}_backend".format(self.stream_direction)
    
    def get_frontend_addr(self):
        return self.frontend_addr
    
    def get_backend_addr(self):
        return self.backend_addr
        
    def set_proxy(self):
        context = zmq.Context(1)
        frontend = context.socket(zmq.ROUTER)
        frontend.bind(self.frontend_addr)
    
        backend  = context.socket(zmq.DEALER)
        backend.bind(self.backend_addr)
    
        zmq.proxy(frontend, backend)
    
        frontend.close()
        backend.close()
        context.term()
        
    def run(self):
        self.set_proxy()
             
if __name__ == '__main__':
    dispatch_id = 0
    backtest_path = os.path.join(root_path,str(dispatch_id))
    callbacks = {}
    
    
    