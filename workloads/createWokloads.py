import yaml

def writeYaml(props, filePath):
    with open(filePath, 'w') as f:
        data = yaml.dump(props, f) 

def getYaml(workloadType,\
             topics,\
             partitionsPerTopic,\
             messageSize,\
             payloadFile,\
             subscriptionsPerTopic,\
             consumersPerSubscription,\
             producersPerTopic,\
             producerRate,\
             testDurationMinutes,\
             #topicChangeIntervalSeconds,\
             kvps):
#fractionTopicsChange):
    conf = {"topics": topics,\
             "partitionsPerTopic": partitionsPerTopic,\
             "messageSize": messageSize,\
             "payloadFile": payloadFile,\
             "subscriptionsPerTopic": subscriptionsPerTopic,\
             "consumerPerSubscription": consumersPerSubscription,\
             "producersPerTopic": producersPerTopic,\
             "producerRate": producerRate,\
             "testDurationMinutes": testDurationMinutes,\
             #"fractionTopicsChange": fractionTopicsChange,\
             "keyDistributor": "NO_KEY",
             "consumerBacklogSizeGB": 0,
             "name" : str(topics) + " topics / " + str(subscriptionsPerTopic) + " subs per topic / " + workloadType    }
    for k in kvps:
        conf[k] = kvps[k]
    return conf



def generateYamlFilesStream(maxTopics, topicIncrement, maxSubscriptions, consumerIncrement, fanout):
    partitionsPerTopic = 1
    messageSize = 1024
    payloadFile = "payload/payload-1Kb.data"
    consumersPerSubscription = 1
    producersPerTopic = 1
    producerRate = 50000
    testDurationMinutes = 3
    topicChangeIntervalSeconds = 9
    for topics in range(1, maxTopics+1, topicIncrement):
         for consumers in range(1, maxSubscriptions+1, consumerIncrement):
             conf = getYaml("stream", topics,\
                            partitionsPerTopic,\
                            messageSize,\
                            payloadFile,\
                            consumers,\
                            consumersPerSubscription,\
                            producersPerTopic,\
                            producerRate,\
                            testDurationMinutes,\
                            {"fanout":fanout})
                            #topicChangeIntervalSeconds,\
                            #fractionTopicsChange)     
             writeYaml(conf , 'testWorkloads/' + str(topics) + '-topics-' + str(consumers) + '-subscriptions-stream.yaml')


def generateYamlFilesChangingSub(maxTopics, topicIncrement, maxSubscriptions, consumerIncrement, fractionTopicsChange):
    partitionsPerTopic = 1
    messageSize = 1024
    payloadFile = "payload/payload-1Kb.data"
    consumersPerSubscription = 1
    producersPerTopic = 1
    producerRate = 50000
    testDurationMinutes = 3
    topicChangeIntervalSeconds = 9
    
    for topics in range(2, maxTopics+1, topicIncrement):
         for consumers in range(1, maxSubscriptions+1, consumerIncrement):
             conf = getYaml("changing subscription", topics,\
                            partitionsPerTopic,\
                            messageSize,\
                            payloadFile,\
                            consumers,\
                            consumersPerSubscription,\
                            producersPerTopic,\
                            producerRate,\
                            testDurationMinutes,\
                            {"topicChangeIntervalSeconds":topicChangeIntervalSeconds,\
                            "fractionTopicsChange": fractionTopicsChange})     
             writeYaml(conf , 'testWorkloads/' + str(topics) + '-topics-' + str(consumers) + '-subscriptions-moving.yaml')

if __name__ == '__main__':
    #generateYamlFilesChangingSub( 10, 2, 5, 1, 0.2)
    generateYamlFilesStream(4, 1, 1, 1, 2)
