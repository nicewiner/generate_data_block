import json

base_file = r'E:\autoBackTest\block.config'

text1 = {'start_date':20160101,'end_date':20160102,'indicators':["lastPrice","Volume"],'instruments':['if0001','if0002']}
text2 = {'start_date':20170101,'end_date':20170102,'indicators':["lastPrice","Volume"],'instruments':['if0001','if0002']}

with open(base_file,'w') as fout:
    table = {'0':text1,'1':text2}
    json.dump(table,fout)
    
with open(base_file,'r') as fin:
    readed = json.load(fin)
    print readed