
def dict_to_lower(d):
    l = {}
    for k,v in d.iteritems():
        if not isinstance(v,str):
            l[k] = v
        else:
            l[k] = str.lower(v)
    return l