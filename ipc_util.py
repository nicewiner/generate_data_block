import zmq
import subprocess
import threading

class RPC():
    
    def __init__(self):
        pass
    
class IpcProxy(threading.Thread):
    
    def __init__(self,addr):
        super(IpcProxy, self).__init__()
        self.addr = addr
        
    def set_proxy(self):
        context = zmq.Context()
        frontend = context.socket(zmq.ROUTER)
        frontend.bind(r"ipc://~/tmp/frontend_" + self.addr)
    
        # Socket facing services
        backend  = context.socket(zmq.DEALER)
        backend.bind(r"ipc://~/tmp/backend_" + self.addr)
    
        zmq.proxy(frontend, backend)
    
        frontend.close()
        backend.close()
        context.term()
        
    def run(self):
        self.set_proxy()
        
class Backend(object):
    
    def __init__(self,addr):
        self.addr = r"ipc://~/tmp/backend_" + addr
    
    def execute(self,cmd):
        newP = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        print newP.pid
        while True:  
            buff = newP.stdout.readline()  
            if buff == '' and newP.poll() != None:  
                break 
            else:
                print buff.strip()
                
class Frontend(object):
    
    def __init__(self,addr):
        self.addr = r"ipc://~/tmp/frontend_" + addr
        
        
if __name__ == '__main__':
    backend = Backend()
    backend.execute('ls')
    
    