import yaml

def writeYaml(props, filePath):
    with open(filePath, 'w') as f:
        data = yaml.dump(props, f) 

def getYaml(topics,\
             partitionsPerTopic,\
             messageSize,\
             payloadFile,\
             subscriptionsPerTopic,\
             consumersPerSubscription,\
             producersPerTopic,\
             producerRate,\
             testDurationMinutes,\
             topicChangeIntervalSeconds,\
             fractionTopicsChange):
    conf = {"topics": topics,\
             "partitionsPerTopic": partitionsPerTopic,\
             "messageSize": messageSize,\
             "payloadFile": payloadFile,\
             "subscriptionsPerTopic": subscriptionsPerTopic,\
             "consumersPerSubscription": consumersPerSubscription,\
             "producersPerTopic": producersPerTopic,\
             "producerRate": producerRate,\
             "testDurationMinutes": testDurationMinutes,\
             "topicChangeIntervalSeconds": topicChangeIntervalSeconds,\
             "fractionTopicsChange": fractionTopicsChange,\
             "keyDistributor": "NO_KEY",
             "consumerBacklogSizeGB": 0,
             "name" : str(topics) + " topics / " + str(subscriptionsPerTopic) + " subs per topic / moving"   }

    return conf


def generateYamlFiles(maxTopics, topicIncrement, maxSubscriptions, consumerIncrement, fractionTopicsChange):
    partitionsPerTopic = 1
    messageSize = 1024
    payloadFile = "payload/payload-1Kb.data"
    consumersPerSubscription = 1
    producersPerTopic = 1
    producerRate = 50000
    testDurationMinutes = 3
    topicChangeIntervalSeconds = 30
    
    for topics in range(1, maxTopics, topicIncrement):
         for consumers in range(1, maxSubscriptions, consumerIncrement):
             conf = getYaml(topics,\
                            partitionsPerTopic,\
                            messageSize,\
                            payloadFile,\
                            consumers,\
                            consumersPerSubscription,\
                            producersPerTopic,\
                            producerRate,\
                            testDurationMinutes,\
                            topicChangeIntervalSeconds,\
                            fractionTopicsChange)     
             writeYaml(conf + '_' + str(topics) + '-topics-' + str(consumers) + '-subscriptions.yaml')

if __name__ == '__main__':
    generateYamlFiles( 10, 2, 5, 1, 0.2)
