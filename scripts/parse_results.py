import argparse
import json
import matplotlib
matplotlib.use('agg')
from matplotlib import pyplot as plt
import numpy as np
from os import listdir
from os.path import join, isfile
import pandas as pd
from sortedcontainers import SortedDict

client_info = {}
def parse_file(filename, full_path):
    f = open(join(full_path))
    data = json.load(f)

    values = [min(data['publishRate']), min(data['consumeRate']),
            data['aggregatedPublishLatency95pct'], data['aggregatedEndToEndLatency95pct'],
            data['aggregatedsubscriptionChangeLatency95pct'], data['messagesSent'],
            data['messagesReceived'], len(data['allProducerTopics'].keys())]
    values = np.array(values, dtype='float')

    s = filename.split('-')
    if s[0] == 'C':
        if s[1] not in client_info.keys():
            client_info[s[1]] = np.zeros(8)
        client_info[s[1]] = np.add(client_info[s[1]], values)

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


def create_graphs(outdir, workers):
    client_info_s = SortedDict(client_info)
    for k in client_info_s.keys():
        temp = client_info_s[k][:4]/workers 
        client_info_s[k] = np.append(temp, client_info_s[k][4:])
    
    # legends = ['pub_rate', 'cons_rate', 'pub_latency', 'cons_latency', 'sub_change_latency',
    #     'msg_sent', 'msg_received', 'total_clients']
    x = list(range(len(client_info_s.keys())))
    data = np.array(list(client_info_s.values()))
    plot_graphs(['pub_latency', 'cons_latency'], data, 2, 4, outdir, 'latency.png', x,
        client_info_s.keys())
    plot_graphs(['pub_rate'], data, 0, 1, outdir, 'pub_throughput.png', x,
        client_info_s.keys())
    plot_graphs(['cons_rate'], data, 1, 2, outdir, 'cons_throughput.png', x,
        client_info_s.keys())
    
def parse_results(indir, outdir, workers):
    files = list()
    for i in range(workers):
        dir = join(indir, 'worker'+str(i))
        files.append([])
        for x in listdir(dir):
            if isfile(join(dir, x)) and x.endswith(".json"):
                files[i].append(x)
    for i in range(len(files)):
        for f in files[i]:
            dir = join(indir, 'worker'+str(i))
            parse_file(f, join(dir, f))
    create_graphs(outdir, workers)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", dest="indir", help="Input directory")
    parser.add_argument("-o", dest="outdir", help="Output directory")
    parser.add_argument("-w", dest="workers", help="Number of workers")

    args = parser.parse_args()

    parse_results(args.indir, args.outdir, int(args.workers))