import create_workloads as cw
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

    # sumo = read_config['sumo']
    # files, filename = find_files(sumo['fcd_output'])
    # if filename not in files:
    # 	os.system('sumo -c ' + sumo['scenario'] + ' --step-length ' + str(sumo['step_length']) +
    #     	' --fcd-output ' + sumo['fcd_output'] + ' --fcd-output.geo --begin ' + str(sumo['begin']) +
	#         ' --end ' + str(sumo['end']))
    
    workload = read_config['workload']
    index_config = read_config['index_config']
    
    # parse = read_config['parse']
    # files, filename = find_files(parse['fcd_output'])
    # if filename not in files:
    #     index_config['minLat'], index_config['minLng'], index_config['maxLat'], index_config['maxLng'] =\
    #         pfo.main(parse['fcd_output'], parse['ouput_dir'], parse['low_time'], parse['high_time'])

    split = read_config['split']
    sd.main(split['input_file'], split['output_dir'], split['num_workers'])

    # payloadSizes = ['1Kb', '2Kb', '4Kb']
    # messageSizes = [1024, 2048, 4096]
    # partitionsPerTopic = [1, 8, 16]
    payloadSizes = ['1Kb']
    messageSizes = [1024]
    partitionsPerTopic = [1]
    numClients = [10]

    for pi in range(len(payloadSizes)):
        workload['payloadFile'] = payloadSizes[pi]
        workload['messageSize'] = messageSizes[pi]
        for pt in partitionsPerTopic:
            for c in numClients:
                workload['partitionsPerTopic'] = pt
                workload['numClients'] = c
                workload_filename = cw.generateYamlFiles(workload, index_config)
                automation = read_config['automation']
                os.system('bash run_benchmark.sh {} {} {} {} {} {} {}'.format(automation['benchmark_home'],
                    automation['pulsar_home'], automation['broker'], "\"{}\"".format(automation['clients']),
                    automation['driver_config'], automation['data_dir'], workload_filename))
    
if __name__ == '__main__':
    main(sys.argv[1])
