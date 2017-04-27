import subprocess

def dict_to_lower(d):
    l = {}
    for k,v in d.iteritems():
        if not isinstance(v,str):
            if isinstance(v,list):
                l[k] = map(lambda x:str.lower(x),v)
            else:
                l[k] = v
        else:
            l[k] = str.lower(v)
    return l

def unicode2str(d):
    l = {}
    for k,v in d.iteritems():
        if not isinstance(v,str):
            if isinstance(v,list):
                l[str(k)] = map(lambda x:str(x),v)
            else:
                l[str(k)] = v
        else:
            l[str(k)] = str.lower(v)
    return l

def migrate_sql():
    dbname = 'cffex_if'
    dbpath = r'/media/xudi/My Passport/samba/autoBackTest/cffex_if'
    import os
    os.chdir(dbpath)
    sqls = os.listdir(dbpath)
    for isql in sqls:
        cmd = 'mysql -uxudi -p123456 {0} < {1}'.format(dbname,isql)
        print cmd
        os.system(cmd)

def get_active_IPC():
    ret = subprocess.Popen(['ipcs','-m'],stdout=subprocess.PIPE)
    buf = ret.stdout.read()
    lines = buf.strip().split('\n')[2:]
    activeIPCs = set([line.split()[0] for line in lines])
    return activeIPCs

def ipc_exist(ipc_key):
    ret = subprocess.Popen(['ipcs','-m'],stdout=subprocess.PIPE)
    buf = ret.stdout.read()
    lines = buf.strip().split('\n')[2:]
    activeIPCs = set([line.split()[0] for line in lines])
    return (ipc_key in activeIPCs)

def ipc_remove(*ipcs):
    for ipc_key in ipcs:
        args = ['ipcrm','-M']
        args.append(ipc_key)
        ret = subprocess.call(args)

def histrun_subprocess():
#     cmd = '/quant/bin/histrun -realtime 0 -futureMode 1 -tick 0x0f0f0000 -log /quant/config/LogManager.conf -comSet /quant/config/histrun/autoBackTest/commoditySet.csv -config /quant/config/histrun/autoBackTest/test.config'
    cmd = 'python ~/tmp/sleep.py'
    p=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)  
    line = 0
    while True:  
        buff = p.stdout.readline()  
        print buff
        if buff == '' and p.poll() != None:  
            break  
        line += 1
        
def matplotlib_plot():
    import matplotlib.pyplot as plt
    import pandas as pd
    from matplotlib.dates import DateFormatter
    dates = pd.date_range('20100101','20100601')
    datas = range(len(dates))
    fig = plt.figure(figsize = (18,12))
    ax1 = fig.add_subplot(1,1,1)
    ax1.xaxis.set_major_formatter(DateFormatter('%Y.%m.%d'))
    ax1.plot(dates,datas)
    plt.show()
    
if __name__ == '__main__':
    matplotlib_plot()
    