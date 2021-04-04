import yaml

def writeYaml(props, filePath):
    with open(filePath, 'w+') as f:
        data = yaml.dump(props, f) 

def getYaml(workload_config, index_config):
    conf = {"topics": workload_config['topics'],\
             "partitionsPerTopic": workload_config['partitionsPerTopic'],\
             "messageSize": workload_config['messageSize'],\
             "payloadFile": 'payload/payload-' + workload_config['payloadFile'] + '.data',\
             "consumerPerSubscription": workload_config['consumerPerSubscription'],\
             "producersPerTopic": workload_config['producersPerTopic'],\
             "producerRate": workload_config['producerRate'],\
             "keyDistributor": "NO_KEY",\
             "consumerBacklogSizeGB": 0,\
             "testDurationMinutes": workload_config['testDurationMinutes'],\
             "numClients": workload_config['numClients'],\
             "name" : str(workload_config['producersPerTopic']) + ' procucer / ' +
                str(workload_config['consumerPerSubscription']) + ' on ' + str(workload_config['topics']) +
                ' topic',\
             "indexConfig" : {
                 "blockSize" : index_config['blockSize'],
                 "indexType" : index_config['indexType'],
                 "minX": index_config['minLat'],
                 "minY": index_config['minLng'],
                 "maxX": index_config['maxLat'],
                 "maxY": index_config['maxLng']
                }
            }
    return conf

def generateYamlFiles(workload_config, index_config):
    conf = getYaml(workload_config, index_config)
    outfile_name = '/home/cetus/new-openmessaging-benchmark/workloads/testWorkloads/' +\
        str(workload_config['topics']) +\
        '-topics-' + str(workload_config['partitionsPerTopic']) +\
        '-partitions-' + workload_config['payloadFile'] + '.yaml'
    writeYaml(conf, outfile_name)
    return outfile_name
