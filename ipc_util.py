import zmq
import threading
import os
import argparse
import time
from create_shm import create_shm_and_load_data
from data_center_config import write_db
from backtest import call_backtest
from pta import show_charts
from redis_api import ipc_db_api
from misc import get_today

root_path = r'/home/xudi/autoBackTest'

class PRC_Clinet(threading.Thread):
    
    def __init__(self,username):
        super(PRC_Clinet, self).__init__()
        self.username = username
        self.pid      = os.getpid()
        self.redis_api = ipc_db_api(db_addr = '127.0.0.1',db_port = 6379,db_name = 3)
        
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
            print recv_obj
            status = recv_obj['status']
            cmd    = recv_obj['key']
            if status:
                redis_key = self.redis_api.get_key(cmd['date'], cmd['username'], cmd['pid'], cmd['requestID'], cmd['funcName'])
                self.redis_api.set_value(redis_key,str(status == 0))
                
    def run(self):
        self.recv_back()
        
class RPC_Server(threading.Thread):
    
    def __init__(self,callbacks):
        super(RPC_Server, self).__init__()
        self.pid = os.getpid()
        self.redis_api = ipc_db_api()
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
            print cmd
            argkws = cmd['paras']
            if (cmd is not None) and ( cmd['funcName'] in self.callbacks ):
                ret = self.callbacks[cmd['funcName']](argkws)
                key = {k:v for k,v in cmd.iteritems() if k != 'paras'}
                send_pyobj = {'key':key,'status':ret == 0}
                
                redis_key = self.redis_api.get_key(cmd['date'], cmd['username'], cmd['pid'], cmd['requestID'], cmd['funcName'])
                self.redis_api.set_value(redis_key,str(ret))
    
                self.send_socket.send_json(send_pyobj)
                
    def run(self):
        self.recv_cmd()   
    
class IpcProxy(threading.Thread):
    
    def __init__(self,direction = 'web2server'):
        super(IpcProxy, self).__init__()
        if direction == 'web2server':
            self.stream_direction = 'web2server'
        else:
            self.stream_direction = 'server2web'
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

def download_global_config(**argkws):
    pass

def upload_backetest_result(**argskws):
    pass

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

def web_server_start():
    
    username = 'xudi'
    rpc_client = PRC_Clinet('xudi')
    
    uploadProxy = IpcProxy('server2web')
    recv_addr = uploadProxy.backend_addr
    
    downloadProxy = IpcProxy('web2server')
    send_addr = downloadProxy.frontend_addr
    
    rpc_client.connect(recv_addr, send_addr)
    
    requestID = 0
    funcName  = 'create_shm'
    cmd = rpc_client.create_cmd(requestID, funcName)
    rpc_client.send_cmd(cmd)

def cpp_server_start(block = True):
    
    callbacks = {}
    callbacks['creat_shm'] = create_shm_block
    callbacks['backtest'] = backtest
    
    cpp_server = RPC_Server(callbacks)
    
    uploadProxy = IpcProxy('server2web')
    send_addr = uploadProxy.frontend_addr
    
    downloadProxy = IpcProxy('web2server')
    recv_addr = downloadProxy.backend_addr
    
    cpp_server.connect(recv_addr, send_addr)
    
    cpp_server.start()
    
    if block:
        while True:
            time.sleep(10)

def set_proxy(proxy_type = 'web2server',block = True):
    
    proxy = IpcProxy(proxy_type)
    proxy.start()
    
    if block:
        while True:
            time.sleep(10)
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-upProxy',dest = 'upStream',action = 'store_true',default = False)
    parser.add_argument('-downProxy',dest = 'downStream',action = 'store_true',default = False)
    parser.add_argument('-cppServer',dest = 'cppServer',action = 'store_true',default = False)
    parser.add_argument('-webServer',dest = 'webServer',action = 'store_true',default = False)
    args = parser.parse_args()
    arg_dict = vars(args)
    
    if arg_dict['upStream']:
        set_proxy('server2web')
    elif arg_dict['downStream']:
        set_proxy('web2server')
    elif arg_dict['cppServer']:
        cpp_server_start()
    elif arg_dict['webServer']:
        web_server_start() 
    
    return 0
    
    
    
    