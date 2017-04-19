
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