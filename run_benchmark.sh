#!/bin/bash

copy_config_data()
{
    broker_ip=$BROKER
    if [ "X$broker_ip" != "X" ]
    then
        cp $BENCHMARK_HOME/driver-pulsar/pulsar-temp.yaml /tmp/
        sed -i "s/BROKER_IP/${broker_ip}/g" /tmp/pulsar-temp.yaml
        
        i=1
        for client in $CLIENTS
        do
            scp /tmp/pulsar-temp.yaml $client:$BENCHMARK_HOME/driver-pulsar/pulsar-temp.yaml
            filename="$file_beg$i$file_end"
            scp $BENCHMARK_HOME/locations/$filename $client:$BENCHMARK_HOME/locations/$filename
            dirname="$file_beg$i"
            rm -rf output/$dirname
            mkdir output/$dirname
            i+=1
        done
        # TODO Make the SCP target location config
        
    fi
}

start_cluster() 
{
    broker_ip=$1
    pulsar_home=$2
    
    echo "Starting Global zookeeper"
    # ssh $broker_ip "$pulsar_home/bin/pulsar-daemon start global-zookeeper $pulsar_home/conf/global_zookeeper.conf"
    $pulsar_home/bin/pulsar-daemon start global-zookeeper $pulsar_home/conf/global_zookeeper.conf
    sleep 5

    echo "Starting zookeeper"
    $pulsar_home/bin/pulsar-daemon start zookeeper
    sleep 5

    $pulsar_home/bin/pulsar initialize-cluster-metadata --cluster local --zookeeper ${broker_ip}:2181 --configuration-store ${broker_ip}:2184  --web-service-url http://${broker_ip}:8080/ --broker-service-url pulsar://${broker_ip}:6650/
    sleep 5

    echo "Starting broker"
    $pulsar_home/bin/pulsar-daemon start broker
    sleep 5
}

stop_cluster() 
{
    broker_ip=$1
    pulsar_home=$2
    echo "Stopping Global zookeeper"
    $pulsar_home/bin/pulsar-daemon stop global-zookeeper
    sleep 5

    echo "Stopping zookeeper"
    $pulsar_home/bin/pulsar-daemon stop zookeeper
    sleep 5

    echo "Stopping broker"
    $pulsar_home/bin/pulsar-daemon stop broker

    rm -rf $pulsar_home/data
}

start_benchmark_workers()
{
    i=1
    cd $BENCHMARK_HOME
    ./bin/benchmark --drivers driver-pulsar/pulsar-temp.yaml --locations locations/worker0_data.json workloads/1-topic-16-partitions-1kb.yaml > benchmark.out 2>&1 &

    for client in $CLIENTS
    do
        filename="$file_beg$i$file_end"
        dirname="$file_beg$i"
        ssh $client "cd $BENCHMARK_HOME; ./bin/benchmark --drivers driver-pulsar/pulsar-temp.yaml --locations locations/$filename --outputDir $dirname workloads/1-topic-16-partitions-1kb.yaml > benchmark.out 2>&1 &"
        i+=1
    done
}

stop_benchmark_workers()
{
    pkill java
    for client in $CLIENTS
    do
        ssh $client "pkill java"
    done
}

collect_results()
{
    i=1
    for client in $CLIENTS
    do
        dirname="$file_beg$i"        
        scp -r $client:$BENCHMARK_HOME/dirname $BENCHMARK_HOME/output/
        i+=1
    done
}

BENCHMARK_HOME="/home/surveillance/openmessaging-benchmark"
PULSAR_HOME="/home/surveillance/pulsar/"
BROKER=192.168.124.238
CLIENTS="192.168.124.112"

file_beg="worker"
file_end="_data.json"
main_client=$BROKER
workers=""
# for client in $CLIENTS
# do
#     main_client=$client
#     break
# done

for client_ip in $CLIENTS
do
    addr="http://${client_ip}:8080"
    if [ "$workers" != "" ]
    then
        addr=",${addr}"
    fi
    workers="${workers}${addr}"
done

stop_benchmark_workers
stop_cluster $BROKER $PULSAR_HOME

start_cluster $BROKER $PULSAR_HOME
python3 $BENCHMARK_HOME/split_data.py -i $BENCHMARK_HOME/locations/locations.data -o $BENCHMARK_HOME/locations -w 2
copy_config_data
start_benchmark_workers

sleep 5m

collect_results