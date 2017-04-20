
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
    import subprocess
    ret = subprocess.Popen(['ipcs','-m'],stdout=subprocess.PIPE)
    buf = ret.stdout.read()
    lines = buf.strip().split('\n')[2:]
    activeIPCs = set([line.split()[0] for line in lines])
    return activeIPCs
            
if __name__ == '__main__':
    print get_active_IPC()
    