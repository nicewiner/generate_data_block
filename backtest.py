import os
import subprocess
from misc import get_hourminsec,get_today

block_root_path = r'/home/xudi/autoBackTest'
config_root_path = r'/quant/config/histrun/autoBackTest'

def call_backtest(**argkws):

    dispatch_id,username = argkws['dispatch_id'], argkws['user_name']
    date,tstamp = argkws['date'],argkws['tstamp']
    
    shm_block_path = os.path.join(block_root_path,str(dispatch_id))
    backtest_config_path    = os.path.join(config_root_path,str(dispatch_id))
    
    files = os.listdir(backtest_config_path)
    comset_path = os.path.join(backtest_config_path,filter(lambda x:str.endswith(x,'commoditySet.csv'),files)[0])
    config_path = os.path.join(backtest_config_path,filter(lambda x:str.endswith(x,'test.config'),files)[0])
    
    result_path = os.path.join(shm_block_path,'result')
    if not os.path.exists(result_path):
        os.mkdir(result_path)
    result_path = os.path.join(result_path,username)
    if not os.path.exists(result_path):
        os.mkdir(result_path)
    result_path = os.path.join(result_path,str(date))
    if not os.path.exists(result_path):
        os.mkdir(result_path)
    result_path = os.path.join(result_path,str(tstamp))    
    if not os.path.exists(result_path):
        os.mkdir(result_path)
        
    output_path = os.path.join(result_path,'output.txt')
    ipckey = '0x0f0f%04d' %(int(dispatch_id))
    
    cmd = r'/quant/bin/histrun -realtime 0 -futureMode 1 -tick {0} -log /quant/config/LogManager.conf -comSet {1} -config {2} > {3}'\
             .format(ipckey,comset_path,config_path,output_path)
    print cmd
    curdir = os.curdir
    os.chdir(result_path)
    ret_code = subprocess.call(cmd,shell = True)
    os.chdir(curdir)
    if ret_code == 0:
        print 'successed!'
        return 0
    else:
        print 'failed'
        return -1
    
if __name__ == '__main__':
    
    call_backtest(dispatch_id = 0,user_name = 'xudi',date = get_today(),tstamp = get_hourminsec())
        