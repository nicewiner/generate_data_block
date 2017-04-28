
import pandas as pd
import numpy as np
import os
# from matplotlib import rcParams
# rcParams['font.sans-serif'] = ['SimHei']
# rcParams['font.family'] = 'sans-serif'
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
# plt.rcParams['axes.unicode_minus']=False 

def parser(input):
    exit_conditions = []
    entry_conditions = []
    everyday_detail = []
    summary = []
    std = 0.0
    stage = 0
    with open(input,'r') as fin:
        lines = fin.readlines()
        i,total_rows = 0,len(lines)
        while i < total_rows:
            line = lines[i].strip()
            if stage == 0 and line == 'Detect Exit Condition:':
                stage = 1
                while True:
                    i += 1
                    line = lines[i].strip() 
                    if not ( line.startswith('|') or line.startswith('+') ):
                        break
                    else:
                        exit_conditions.append(line)
            if stage == 1 and line == 'Detect Entry Condition:':
                stage = 2
                while True:
                    i += 1
                    line = lines[i].strip() 
                    if not ( line.startswith('|') or line.startswith('+') ):
                        break
                    else:
                        entry_conditions.append(line)
            if stage == 2 and line == 'Running in Hist Mode:':
                stage = 3
                while True:
                    i += 1
                    line = lines[i].strip() 
                    if not ( line.startswith('|') or line.startswith('+') ):
                        break
                    else:
                        everyday_detail.append(line)
            if stage == 3 and line == 'summary chart:':
                stage = 4
                while True:
                    i += 1
                    line = lines[i].strip() 
                    if not ( line.startswith('|') or line.startswith('+') ):
                        break
                    else:
                        summary.append(line)
            if stage == 4 and line.startswith('Summary:'):
                std = float(line.split(',')[-1].split('=')[-1].strip())
            i += 1
            
    return entry_conditions,exit_conditions,everyday_detail,summary,std

def get_summarys(input):
    with open(input,'r') as fin:
        useful = False
        summarys = []
        for line in fin:
            line = line.strip()
            if line == 'summary chart:':
                useful = True
                continue
            if useful:
                if not ( line.startswith('|') or line.startswith('+') ):
                    break
                else:
                    summarys.append(line)
        if len(summarys) > 0:
            return summarys[3]
    return None

def generate_png_from_summary(exit_path,summary_path,engine_output,charts,fast_mode = True):
        
        if not fast_mode:
            exit_frame = pd.read_csv(exit_path)
            total_trd = exit_frame['TradeLen']
            avg_trd_len = np.mean(total_trd)
            total_num = len(exit_frame)
            win_num   = np.count_nonzero( exit_frame['TrdPnLdlr'] > 0.01 )
#             lose_num  = np.count_nonzero( exit_frame['TrdPnLdlr'] < -0.01)
            win_rate = 0
            if total_num > 0:
                win_rate = float(win_num) / total_num
        else:
            engine_summary = get_summarys(engine_output).strip().split('|')
            avg_trd_len = float(engine_summary[7].strip())
            win_rate    = float(engine_summary[5].strip()) / 100.0
        
        summary_frame = pd.read_csv(summary_path)
        summary_frame['PnLDL'] = summary_frame['PnLDL'].cumsum()
        summary_frame['PnLDS'] = summary_frame['PnLDS'].cumsum()
        
        pnl_out_path = os.path.join(charts,'pnl.png')
        position_out_path = os.path.join(charts,'position.png')
        volatility_path = os.path.join(charts,'volatility.png')
        
        trd_stat = pd.DataFrame(index = ['Avg','Med'], columns = ['trd/d','pos/d','trdlen','Net','Long','Short','turnover','adv','adg'])
        pnl_stat = pd.DataFrame(index = ['Pre','Post'],columns = ['$/perday','$/std','$/sharp'])
        
        ###statistics for trade
        trd_stat['trd/d']['Avg'] = summary_frame['NNTrd'].mean()
        trd_stat['pos/d']['Avg'] = summary_frame['NPOS'].mean()
        trd_stat['trdlen']['Avg'] = avg_trd_len
        trd_stat['Net']['Avg'] = summary_frame['NetExp'].mean()
        trd_stat['Long']['Avg'] = summary_frame['Long'].mean()
        trd_stat['Short']['Avg'] = summary_frame['Short'].mean()
        
#         trd_stat['trd/d']['Med'] = summary_frame['NNTrd'].median()
#         trd_stat['pos/d']['Med'] = summary_frame['NPOS'].median()
#         trd_stat['trdlen']['Med'] = np.median(total_trd) 
#         trd_stat['Net']['Med'] = summary_frame['NetExp'].median()
#         trd_stat['Long']['Med'] = summary_frame['Long'].median()
#         trd_stat['Short']['Med'] = summary_frame['Short'].median()

        ###statistics for pnl
        pnl_stat['$/perday']['Pre'] = summary_frame['PnLD'].mean()
        pnl_stat['$/std']['Pre'] = summary_frame['PnLD'].std()
        pnl_stat['$/sharp']['Pre'] = pnl_stat['$/perday']['Pre'] * np.sqrt(255.0) / pnl_stat['$/std']['Pre']
        
        pnl_stat['$/perday']['Post'] = summary_frame['Post'].mean()
        pnl_stat['$/std']['Post'] = summary_frame['Post'].std()
        pnl_stat['$/sharp']['Post'] = pnl_stat['$/perday']['Post'] * np.sqrt(255.0) / pnl_stat['$/std']['Post']

        date_transf = lambda x:pd.to_datetime(str(x))
        
        time_index = summary_frame['DATE'].apply(date_transf)
        
        fig = plt.figure(figsize = (18,12))
        ax1 = fig.add_subplot(1,1,1)
        ax1.plot(time_index,summary_frame['totPre'].values,linestyle = '-',color = 'red',label = 'totPre')
        ax1.plot(time_index,summary_frame['totPost'].values,linestyle = '-',color = 'blue',label = 'totPost')
        ax1.legend(loc = 'best')
        ax1.set_title('Pre & Post SR = %0.2f, %0.2f, Mean = %0.2f, %0.2f,Std = %0.2f, %0.2f,TrdLen = %0.1f,win rate = %0.2f'\
                       %(pnl_stat['$/sharp']['Pre'],pnl_stat['$/sharp']['Post'],\
                         pnl_stat['$/perday']['Pre'],pnl_stat['$/perday']['Post'],\
                         pnl_stat['$/std']['Pre'],pnl_stat['$/std']['Post'],\
                         trd_stat['trdlen']['Avg'],
                         win_rate) )
        ax1.xaxis.set_major_formatter(DateFormatter('%Y.%m'))
        for tick in ax1.get_xticklabels():
            tick.set_rotation(30)
        
        plt.grid()
        fig.savefig(pnl_out_path,format = 'png')
        plt.clf()
        plt.close()
        
        fig = plt.figure(figsize = (18,12))     
        ax2 = fig.add_subplot(2,2,1)
        ax2.plot(time_index,summary_frame['Long'].values,linestyle = '-',color = 'red',label = 'Long')
        ax2.plot(time_index,summary_frame['Short'].values,linestyle = '-',color = 'blue',label = 'Short')
        ax2.plot(time_index,summary_frame['NetExp'].values,linestyle = '-',color = 'green',label = 'Net')
        ax2.legend(loc = 'best')
        ax2.set_title('Position')
        ax2.xaxis.set_major_formatter(DateFormatter('%Y.%m'))
        plt.grid()
        for tick in ax2.get_xticklabels():
            tick.set_rotation(45)
            
        ax3 = fig.add_subplot(2,2,3)
        ax3.plot(time_index,summary_frame['NNTrd'].values,linestyle = '-',color = 'red',label = 'NNtrd')
        ax3.plot(time_index,summary_frame['NPOS'].values,linestyle = '-',color = 'blue',label = 'NPOS')
        ax3.legend(loc = 'best')
        ax3.set_title('#NNtrd #NPOS')
        ax3.xaxis.set_major_formatter(DateFormatter('%Y.%m'))
        plt.grid()
        for tick in ax3.get_xticklabels():
            tick.set_rotation(45)
        
        ax4 = fig.add_subplot(2,2,2)
        ax4.plot(time_index,summary_frame['PnLDL'].cumsum(),linestyle = '-',color = 'red',label = 'Long PNL')
        ax4.plot(time_index,summary_frame['PnLDS'].cumsum(),linestyle = '-',color = 'blue',label = 'Short PNL')
        ax4.legend(loc = 'best')
        ax4.set_title('Long Short PNL')
        ax4.xaxis.set_major_formatter(DateFormatter('%Y.%m'))
        for tick in ax4.get_xticklabels():
            tick.set_rotation(45)
    
        ax1 = fig.add_subplot(2,2,4)
        ax1.plot(time_index,summary_frame['EntrySlip'].values + summary_frame['ExitSlip'].values,linestyle = '-',color = 'red',label = 'Slipage')
        ax1.legend(loc = 'best')
        ax1.set_title('Slipage')
        ax1.xaxis.set_major_formatter(DateFormatter('%Y.%m'))
        for tick in ax1.get_xticklabels():
            tick.set_rotation(45)
        plt.grid()
        
        fig.savefig(position_out_path,format = 'png')
        plt.clf()
        plt.close()                 
        
        pd.set_option('chained_assignment',None)
        summary_frame.loc[:,'rollingStd'] = summary_frame['Post'].rolling(window=100,center=False).std().values
           
        max_cumsum = 0
        maxs = np.zeros(len(summary_frame.index),dtype = float)
        for i,v in enumerate( summary_frame['totPost'].values ):
            if v > max_cumsum: 
                max_cumsum = v
            maxs[i] = max_cumsum
        
        drop_down = [ i[0] - i[1] for i in zip(summary_frame['totPost'].values,maxs) ]
        summary_frame.loc[:,'drawDown'] = drop_down
        
        fig = plt.figure(figsize = (18,12))
               
        ax1 = fig.add_subplot(3,1,1)
        
        ax1.plot(time_index,summary_frame['totPre'].values,linestyle = '-',color = 'blue',label = 'cumSum')
        ax1.plot(time_index,summary_frame['totPost'].values,linestyle = '-',color = 'green',label = 'post')
        ax1.legend(loc = 'best')
        ax1.set_title('Pre & Post SR = %0.2f, %0.2f, Mean = %0.2f, %0.2f,Std = %0.2f, %0.2f '\
                       %(pnl_stat['$/sharp']['Pre'],pnl_stat['$/sharp']['Post'],\
                         pnl_stat['$/perday']['Pre'],pnl_stat['$/perday']['Post'],\
                         pnl_stat['$/std']['Pre'],pnl_stat['$/std']['Post'],))
        ax1.xaxis.set_major_formatter(DateFormatter('%Y.%m'))
        plt.grid()
        
        ax2 = fig.add_subplot(3,1,2,sharex = ax1)
        ax2.plot(time_index,summary_frame['rollingStd'].values,linestyle = '-',color = 'blue',label = 'RollingStd')
        ax2.legend(loc = 'best')
        ax2.xaxis.set_major_formatter(DateFormatter('%Y.%m'))
        plt.grid()
        
        ax3 = fig.add_subplot(3,1,3,sharex = ax1)
        ax3.plot(time_index,summary_frame['drawDown'].values,linestyle = '-',color = 'blue',label = 'maxDrawDown')
        ax3.legend(loc = 'best')
        ax3.set_ylim(1.2 * summary_frame['drawDown'].min(), -0.2 * summary_frame['drawDown'].min())
        ax3.xaxis.set_major_formatter(DateFormatter('%Y.%m'))   
        plt.grid()
        fig.savefig(volatility_path,format = 'png')
        plt.clf()
        plt.close()

def show_output(basic_path):
    infile = os.path.join(basic_path,'output.txt')
    entrys,exits,every_day,detail,std = parser(infile)
    for row in entrys:
        print row
    for row in exits:
        print row
    for row in every_day:
        print row 
    for row in detail:
        print row
    print std

def show_charts(basic_path):
    files = os.listdir(basic_path)
    exits  = os.path.join(basic_path,filter(lambda x: str.endswith(x,'exit_list.csv'),files)[0])
    summary = os.path.join(basic_path,filter(lambda x: str.endswith(x,'summary.csv'),files)[0])
    output = os.path.join(basic_path,filter(lambda x: str.endswith(x,'output.txt'),files)[0])
    charts = os.path.join(basic_path,'pta')
    if not os.path.exists(charts):
        os.mkdir(charts)
    generate_png_from_summary(exits,summary,output,charts)
    
if __name__ == '__main__':
    basic_path = r'/home/xudi/autoBackTest/0/result/xudi/20170428/115457'
    show_charts(basic_path)
    
        