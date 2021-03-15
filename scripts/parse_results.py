import argparse
import json
import matplotlib
matplotlib.use('agg')
from matplotlib import pyplot as plt
import numpy as np
from os import listdir
from os.path import join, isfile
import pandas as pd
import seaborn as sns
from sortedcontainers import SortedDict

client_info = []
def parse_file(filename, full_path):
    f = open(join(full_path))
    data = json.load(f)

    values = [min(data['publishRate']), min(data['consumeRate']),
            data['aggregatedPublishLatency95pct'], data['aggregatedEndToEndLatency95pct'],
            data['aggregatedsubscriptionChangeLatency95pct'], data['messagesSent'],
            data['messagesReceived'], len(data['allProducerTopics'].keys())]
    # values = np.array(values, dtype='float')

    s = filename.split('_')
    if s[0] == 'C':
        if s[2] == 'I':
            values.insert(0, s[3])
            values.insert(0, s[1])
    
    client_info.append(values)

def plot_graphs(legends, data, l_range, h_range, outdir, outfile, x, x_labels):
    fig, ax = plt.subplots()
    for i in range(l_range, h_range):
        y = data[:,i]
        ax.plot(x, y)
    ax.legend(legends)
    ax.set_xlabel("#clients")
    ax.set_ylabel("time (ms)")
    ax.xaxis.set_ticks(x)
    ax.xaxis.set_ticklabels(x_labels)
    fig.savefig(join(outdir, outfile))

def create_graphs(outdir, workers, df):
    columns = ['pub_rate', 'cons_rate', 'pub_latency', 'cons_latency']
    for col in columns:
        df_col = df[[col]].unstack()
        df_col.plot(kind='line')
        plt.savefig(outdir+'/'+col+'.png')

def create_graphs_1(outdir, workers):
    client_info_s = SortedDict(client_info)
    iters = []
    for k in client_info_s.keys():
        iters = client_info_s[k].keys()
        for i in client_info_s[k].keys():
            temp = client_info_s[k][i][:4]/workers 
            client_info_s[k][i] = np.append(temp, client_info_s[k][i][4:])

    # legends = ['pub_rate', 'cons_rate', 'pub_latency', 'cons_latency', 'sub_change_latency',
    #     'msg_sent', 'msg_received', 'total_clients']
    data = {}
    iter_data = {}
    for k in client_info_s.keys():
        data[k] = np.array(list(client_info_s[k].values()))
        print(data[k])
        plot_graphs(['pub_latency', 'cons_latency'], data[k], 2, 4, outdir, 'latency_{}.png'.format(k),
            list(range(len(iters))), iters)
        for i in iter:
            iter_data
    
    # iter_data = {}
    # for i in iters:
        
    #     plot_graphs(['pub_latency', 'cons_latency'], , 2, 4, outdir, 'latency_{}.png'.format(k),
    #         list(range(len(iters))), iters)

    # plot_graphs(['pub_rate'], data, 0, 1, outdir, 'pub_throughput.png', x,
    #     client_info_s.keys())
    # plot_graphs(['cons_rate'], data, 1, 2, outdir, 'cons_throughput.png', x,
    #     client_info_s.keys())
    
def parse_results(indir, outdir, workers):
    files = list()
    for i in range(workers):
        files.append([])
        for x in listdir(indir):
            if isfile(join(indir, x)) and x.endswith(".json"):
                files[i].append(x)
    for i in range(len(files)):
        for f in files[i]:
            parse_file(f, join(indir, f))
    
    df = pd.DataFrame(client_info, columns = ['num_clients', 'iterations', 'pub_rate', 'cons_rate',
        'pub_latency', 'cons_latency', 'sub_change_latency', 'msg_sent', 'msg_received',
        'total_clients'])
    df = df.groupby(['num_clients', 'iterations']).agg({'pub_rate': np.mean, 'cons_rate': np.mean,
        'pub_latency': np.mean, 'cons_latency': np.mean, 'sub_change_latency': np.mean,
        'msg_sent': np.sum, 'msg_received': np.sum, 'total_clients': np.sum})
    
    create_graphs(outdir, workers, df)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", dest="indir", help="Input directory")
    parser.add_argument("-o", dest="outdir", help="Output directory")
    parser.add_argument("-w", dest="workers", help="Number of workers")

    args = parser.parse_args()

    parse_results(args.indir, args.outdir, int(args.workers))