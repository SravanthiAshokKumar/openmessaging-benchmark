import argparse
import os

clientToWorker = {}
workerTables = []

def split_to_files(infile, outdir, workers):
    for i in range(workers):
        workerTables.append([])
    
    w = 0
    lines = list()
    with open(infile) as ipfile:
        lines = ipfile.readlines()
    
    for line in lines:
        words = line.split(" ")
        if words[0] in clientToWorker:
            workerID = clientToWorker[words[0]]
            workerTables[workerID].append(line)
            if w == workerID:
                w = (w + 1)%workers
        else:
            clientToWorker[words[0]] = w
            workerTables[w].append(line)
            w = (w + 1)%workers
    
    for i in range(workers):
        fileName = os.path.join(outdir, "worker{}_locations.data".format(i))
        with open(fileName, 'w+') as outfile:
            outfile.writelines("%s" % entry for entry in workerTables[i])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--infile", help="path of the file to parse")
    parser.add_argument("-o", "--outdir", help="path of the output directory")
    parser.add_argument("-w", "--workers", help="Number of workers")

    args = parser.parse_args()

    split_to_files(args.infile, args.outdir, int(args.workers))
