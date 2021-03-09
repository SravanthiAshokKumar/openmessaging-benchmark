import argparse
import json
import numpy as np
from os import listdir
from os.path import join, isfile
import pandas as pd

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
            client_info[s[1]] = np.zeroes(7)
        client_info[s[1]] = client_info + values
    print(client_info)

def create_graphs(workers):
    for k in client_info.keys():
        temp = client_info[k][:4]/workers
        client_info[k] = np.append(a, client_info[k][4:])
        client_info[k] = np.insert(client_info[k], 0, int(k)*workers)
    df = pd.DataFrame(client_info.values(), columns = ['total_client', 'pub_rate',
        'cons_rate', 'pub_latency', 'cons_latency', 'msg_sent', 'msg_received'])
    print(df)

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
    create_graphs(workers)    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", dest="indir", help="Input directory")
    parser.add_argument("-o", dest="outdir", help="Output directory")
    parser.add_argument("-w", dest="workers", help="Number of workers")

    args = parser.parse_args()

    parse_results(args.indir, args.outdir, int(args.workers))