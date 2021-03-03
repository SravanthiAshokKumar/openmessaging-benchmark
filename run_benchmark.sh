#!/bin/bash

copy_config_data()
{
    broker_ip=$BROKER
    if [ "X$broker_ip" != "X" ]
    then
        cp $BENCHMARK_HOME/driver-pulsar/pulsar-temp.yaml /tmp/
        sed -i "s/BROKER_IP/${broker_ip}/g" /tmp/pulsar-temp.yaml
        
        i=0
        for client in $CLIENTS
        do
            scp /tmp/pulsar-temp.yaml $client:$BENCHMARK_HOME/driver-pulsar/pulsar-temp.yaml
            filename="$file_beg$i$file_end"
            scp $BENCHMARK_HOME/locations/$filename $client:$BENCHMARK_HOME/locations/$filename
            dirname="$file_beg$i"
            ssh $client "rm -rf $BENCHMARK_HOME/output/$dirname; mkdir $BENCHMARK_HOME/output/$dirname"
            i+=1
        done
        
    fi
}

start_cluster() 
{
    broker_ip=$1
    pulsar_home=$2
    
    echo "Starting Global zookeeper"
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
    i=0
    
    for client in $CLIENTS
    do
        filename="$file_beg$i$file_end"
        dirname="$file_beg$i"
        ssh $client "cd $BENCHMARK_HOME; ./bin/benchmark --drivers driver-pulsar/$driver_config \
            --locations locations/$filename --outputDir output/$dirname \
            workloads/1-topic-16-partitions-1kb.yaml > benchmark.out 2>&1 &"
        i+=1
    done
}

stop_benchmark_workers()
{
    for client in $CLIENTS
    do
        ssh $client "pkill java"
    done
}

collect_results()
{
    i=0
    for client in $CLIENTS
    do
        dirname="$file_beg$i"        
        scp -r $client:$BENCHMARK_HOME/output/$dirname $BENCHMARK_HOME/output/
        i+=1
    done
}

BENCHMARK_HOME=$1
PULSAR_HOME=$2
BROKER=$3
CLIENTS=$4
driver_config=$5

file_beg="worker"
file_end="_data.json"
workers=""

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
copy_config_data
start_benchmark_workers

sleep 5m

collect_results