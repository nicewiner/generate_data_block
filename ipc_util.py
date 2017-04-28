import zmq
import threading
import os
from create_shm import create_shm_and_load_data
from data_config_api import write_db
from backtest import call_backtest
from pta import show_charts
from misc import get_today, get_hourminsec

root_path = r'/home/xudi/autoBackTest'

class RPC_CLIENT(object):
    
    def __init__(self,username):
        self.username = username
        self.pid      = os.getpid()
        
    def connect(self,recv_addr,send_addr):
        self.ctx  = zmq.Context(1)
        
        self.send_socket = self.ctx.socket(zmq.PUSH)
        self.send_socket.connect(send_addr)
        
        self.recv_socket = self.ctx.socket(zmq.PULL)
        self.recv_socket.connect(recv_addr)
        
    def create_cmd(self,requestID,funcName,**argkws):
        today = get_today()
        key = '{0}:{1}:{2}:{3}'.format(today,self.username,requestID,funcName)
        hashv = hash(key)
        extra_para_dict = {'date':today,'username':self.username,\
                           'requestID':requestID,'funcName':funcName,\
                           'hashv':hashv,'pid':self.pid
                           }
        extra_para_dict['paras'] = argkws
        return extra_para_dict
        
    def send_cmd(self,cmd):
        self.socket.send_json(cmd)
        
    def recv_back(self):
        while True:
            recv_obj = self.recv_socket.recv_json()
            status = recv_obj['status']
            if status:
                pass
        
class RPC_SERVER(threading.Thread):
    
    def __init__(self,callbacks):
        super(RPC_SERVER, self).__init__()
        self.pid = os.getpid()
        self.callbacks = callbacks
        
    def connect(self,recv_addr,send_addr):
        self.ctx  = zmq.Context(1)
        
        self.recv_socket = self.ctx.socket(zmq.PULL)
        self.recv_socket.connect(recv_addr)
        
        self.send_socket = self.ctx.socket(zmq.PUSH)
        self.send_socket.connect(send_addr)
        
    def verify_cmd(self,cmd):
        try:
            today,username,requestID,funcName,hashv = cmd['date'],cmd['username'],cmd['requestID'],cmd['funcName'],cmd['hashv']
            if str(hash(':'.join((today,username,requestID,funcName)))) != hashv:
                return None
            else:
                return cmd
        except:
            return None
       
    def recv_cmd(self):
        while True:
            cmd = self.recv_socket.recv_json()
            argkws = cmd['paras']
            if cmd is not None:
                ret = self.callbacks[cmd['funcName']](argkws)
                key = ':'.join( (cmd['date'],cmd['username'],cmd['pid'],cmd['requestID'],cmd['funcName']))
                send_pyobj = {'key':key,'status':ret == 0}
                self.send_socket.send_json(send_pyobj)
                
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

## send dispatch_id to cli
def save_shm_block_config(**argkws):
    ret = write_db(argkws)
    return ret

## send create status to cli
def create_shm_block(**argkws):
    ret = create_shm_and_load_data(argkws)
    return ret

def histrun(**argkws):
    ret = call_backtest(**argkws)
    return ret

def pta(**argkws):
    ret = show_charts(**argkws)
    return ret

#do backtest
def backtest(**argkws):
    ret = histrun(argkws)
    if ret == 0:
        ret2 = pta(argkws)
        return ret2
    return ret

if __name__ == '__main__':
    callbacks = {}
    
    
    
    