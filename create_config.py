import json

base_file = r'E:\autoBackTest\block.config'

text = {'start_date':20170101,'end_date':20170102,'indicators':["lastPrice","Volume"],'instruments':['if0001','if0002']}

with open(base_file,'a+') as fout:
    readed = json.dump(fout,text)