#!/bin/bash

copy_config_data()
{
    broker_ip=$BROKER
    cp $BENCHMARK_HOME/driver-pulsar/pulsar-temp.yaml /tmp/
    sed -i "s/BROKER_IP/${broker_ip}/g" /tmp/pulsar-temp.yaml

    if [ "X$broker_ip" != "X" ]
    then
        i=0
        for client in $CLIENTS
        do
            scp /tmp/pulsar-temp.yaml $client:$BENCHMARK_HOME/driver-pulsar/pulsar-temp.yaml
            filename="$file_beg$i$file_end"
            scp $BENCHMARK_HOME/$DATA_DIR/$filename $client:$BENCHMARK_HOME/locations/$filename
            scp $WORKLOAD_FILENAME $client:$BENCHMARK_HOME/workloads/testWorkloads/
            dirname="$file_beg$i"
            ssh $client "rm -rf $BENCHMARK_HOME/output/$dirname; mkdir $BENCHMARK_HOME/output/$dirname"
            i=$((i+1))
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

    $pulsar_home/bin/pulsar-admin tenants create benchmark
    $pulsar_home/bin/pulsar-admin namespaces create benchmark/ns-names
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
        if [ "$i" -eq 0 ]
        then
            ssh $client "cd $BENCHMARK_HOME; ./bin/app-benchmark.sh --drivers driver-pulsar/$DRIVER_CONFIG \
            --outdir output/$dirname indexConfig.yaml > output/$dirname/application-benchmark.out &"
        else
            ssh $client "cd $BENCHMARK_HOME; ./bin/benchmark --drivers driver-pulsar/$DRIVER_CONFIG \
                --locations locations/$filename --outputDir output/$dirname \
                $WORKLOAD_FILENAME > output/$dirname/benchmark.out &"
        fi
        i=$((i+1))
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
        scp -r $client:$BENCHMARK_HOME/output/$dirname $BENCHMARK_HOME/output/output_static_trial/$dirname
        cd $BENCHMARK_HOME/output/output_static_trial/$dirname/
        f=(`ls`)
        
        if [ "$i" -eq 0 ]
        then
            app_json=json_filename=application_C_$NUM_CLIENTS"_I_"$ITER"_"$dirname.json 
            app_out=application_C_$NUM_CLIENTS"_I_"$ITER"_"$dirname.out
            cp ${f[0]} ../$app_json
            cp ${f[1]} ../$app_out
        else
            json_filename=C_$NUM_CLIENTS"_I_"$ITER"_"$dirname.json
            out_filename=C_$NUM_CLIENTS"_I_"$ITER"_"$dirname.out
            cp ${f[0]} ../$json_filename
            cp ${f[1]} ../$out_filename
        fi
        cd ..
        rm -rf $dirname
        cd $BENCHMARK_HOME
        scp -r $client:$BENCHMARK_HOME/benchmark.out $BENCHMARK_HOME/output/output_static_trial/$out_filename
        i=$((i+1))
    done
}

BENCHMARK_HOME=$1
PULSAR_HOME=$2
BROKER=$3
CLIENTS=$4
DRIVER_CONFIG=$5
DATA_DIR=$6
WORKLOAD_FILENAME=$7
NUM_CLIENTS=$8
ITER=$9

file_beg="worker"
file_end="_locations.data"
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

sleep 4m

collect_results
