import create_workloads as cw
import generate_dynamic_locations as gdl
import os
import parse_fcd_output as pfo
import split_data as sd
import subprocess
import sys
import yaml

def find_files(output_file):
    split_names = output_file.split('/')
    filename = split_names[len(split_names)-1]
    split_names.remove(filename)
    files = os.listdir(os.path.join(*split_names))
    return files, filename

def main(configFile):
    read_config = {}
    with open(configFile, 'r+') as file:
        read_config = yaml.load(file, Loader=yaml.FullLoader)

    workload = read_config['workload']
    index_config = read_config['index_config']
    
    locations_config = read_config['locations_config']
    index_config['minLat'], index_config['minLng'], index_config['maxLat'], index_config['maxLng'] =\
        locations_config['min_lat'], locations_config['min_lng'], locations_config['max_lat'], locations_config['max_lng']
    numStaticClients = [10]
    # numMobileClients = [4]
    iterations = [1]
    outfile_prefix = '/home/cetus/new-openmessaging-benchmark/parsed/locations/static_locations_sc_'

    for sc in numStaticClients:
        # for mc in numMobileClients:
        # outfile = outfile_prefix + str(sc) + '_mc_' + str(mc) + '.data'
        outfile = outfile_prefix + str(sc) + '.data'
        if os.path.exists(outfile):
            os.remove(outfile)
        
        gdl.generate_location_data(sc, 0, locations_config['min_lat'], locations_config['min_lng'],
            locations_config['max_lat'], locations_config['max_lng'], locations_config['locChangeInterval'],
            locations_config['totalDuration'], outfile)
        split = read_config['split']
        sd.split_to_files(outfile, split['output_dir'], split['num_workers'])

        for i in iterations:
            # workload['numClients'] = sc + mc
            workload['numClients'] = sc
            payloadSizes = ['1Kb']
            messageSizes = [1024]
            partitionsPerTopic = [1]

            for ps,ms in zip(payloadSizes, messageSizes):
                workload['payloadFile'] = ps
                workload['messageSize'] = ms
                for pt in partitionsPerTopic:
                    workload['partitionsPerTopic'] = pt
                    workload_filename = cw.generateYamlFiles(workload, index_config)
                    automation = read_config['automation']
                    os.system('bash run_application_benchmark.sh {} {} {} {} {} {} {} {} {}'.format(
                        automation['benchmark_home'], automation['pulsar_home'], automation['broker'],
                        "\"{}\"".format(automation['clients']), automation['driver_config'],
                        automation['data_dir'], workload_filename, str(sc), i))

if __name__ == '__main__':
    main(sys.argv[1])
