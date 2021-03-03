#1. create a config file
#2. run sumo scenario with required config and parse it to locations.data
# sumo -c scenario/dua.static.sumocfg --step-length 1.0 --fcd-output fcd-output.txt
#     --fcd-output.geo --end 28100 --begin 27000
# python3 parse_fcd_output.py -O . --fcd-output ../LuSTScenario/fcd-output.txt 
#     --low-time 0 --high-time 1000000
#3. run split_data on locations.data generated
# python3 $BENCHMARK_HOME/split_data.py -i $BENCHMARK_HOME/locations/locations.data 
#     -o $BENCHMARK_HOME/locations -w 2
#4. replace workload config yaml with min, max found in locations.data
#5. run the automation script
#6. change run_benchmark.sh -> remove split_data, have config for all hard-coded values 

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

    sumo = read_config['sumo']
    files, filename = find_files(sumo['fcd_output'])
    # if filename not in files:
    os.system('sumo -c ' + sumo['scenario'] + ' --step-length ' + str(sumo['step_length']) +
        ' --fcd-output ' + sumo['fcd_output'] + ' --fcd-output.geo --begin ' + str(sumo['begin']) +
        ' --end ' + str(sumo['end']))
    
    workload = read_config['workload']
    index_config = read_config['index_config']
    
    parse = read_config['parse']
    files, filename = find_files(parse['fcd_output'])
    # if filename not in files:
    index_config['minLat'], index_config['minLng'], index_config['maxLat'], index_config['maxLng'] =\
        pfo.main(parse['fcd_output'], parse['ouput_dir'], parse['low_time'], parse['high_time'])

    split = read_config['split']
    sd.main(split['input_file'], split['output_dir'], split['num_workers'])

    cw.generateYamlFiles(workload, index_config)

    automation = read_config['automation']
    os.system('bash run_benchmark.sh {} {} {} {} {} {}'.format(automation['benchmark_home'],
        automation['pulsar_home'], automation['broker'], automation['clients'],
        automation['driver_config'], automation['data_dir']))
    
if __name__ == '__main__':
    main(sys.argv[1])