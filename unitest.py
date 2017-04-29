import zmq
from ipc_util import getUploadAddr_front,getUploadAddr_back,getDownloadAddr_back,getDownloadAddr_front
import time
    
def test_proxy():
    
    ctx = zmq.Context()
    
    send_socket = ctx.socket(zmq.PUSH)
    recv_socket = ctx.socket(zmq.PULL)
    
    send_socket.set_hwm(0)
    recv_socket.set_hwm(0)
    
    send_socket.connect(getDownloadAddr_front())
    recv_socket.connect(getDownloadAddr_back())
    
    i = 0
    while True:
        send_socket.send(str(i))
        msg = recv_socket.recv()
        print msg
        time.sleep(1)
        i += 1
    
if __name__ == '__main__':
    
    test_proxy()
    
        
    