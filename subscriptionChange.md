## Measuring Subscription Change Times on Kafka and Pulsar  
The code paths have been changed to accept a subscription change interval and fraction topics change to periodically change aconsumer's subscriptions.  
To test these changes, use a YAML file which specifies `fractionTopicsChange` and `topicChangeIntervalSeconds`.  
The workload/ directory has a createWorkload.py script which can automatically generate workloads to test subscription changes.  
To run the benchmark for subscription changes use  
```shell 
$ bash run.sh path/to/workload/dir (kafka|pulsar) 
```
The results are generated in the same json format, with additional information about subscription changes.
