/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.openmessaging.benchmark.driver.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.Arrays;
import java.util.Set;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.ArrayList;
import java.util.OptionalDouble;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.PartitionInfo;

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.time.StopWatch;

public class KafkaBenchmarkConsumer implements BenchmarkConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaBenchmarkConsumer.class);

    private final KafkaConsumer<String, byte[]> consumer;
    private AtomicBoolean isSubscriptionChanged = new AtomicBoolean(false);
    private String subscription;

    private final ExecutorService executor;
    private final Future<?> consumerTask;
    private volatile boolean closing = false;

    private ArrayList<Double> subscriptionChangeTime = new ArrayList<Double>();
    public KafkaBenchmarkConsumer(KafkaConsumer<String, byte[]> consumer, ConsumerCallback callback) {
        this.consumer = consumer;
        this.executor = Executors.newSingleThreadExecutor();

        this.consumerTask = this.executor.submit(() -> {
            while (!closing) {
                try {
                    if(isSubscriptionChanged.get()){
                        log.info("Changing subscription");
                        Set<String> topics = consumer.listTopics().keySet();

                        StringBuffer subs = new StringBuffer();
                        for(String topic : topics) {
                            subs = subs.append(topic).append(",");
                        }
                        log.info("Current subscriptions = {}", subs.toString());
                        log.info("sub = {}", subscription);           

                        StopWatch sw = new StopWatch();
                        sw.start();
                        unsubscribe();
                        subscribe();
                        sw.stop();
                        log.info("subscription change took {} ms", sw.getTime());
                        topics.clear();
                        topics = consumer.listTopics().keySet();

                        subs.setLength(0);
                        for(String topic : topics) {
                            subs = subs.append(topic).append(",");
                        }
                        log.info("Changed subscriptions = {}", subs.toString());
          
                        subscriptionChangeTime.add((double)sw.getTime());
                    }
                    Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
                    ConsumerRecords<String, byte[]> records = consumer.poll(100);

                    for (ConsumerRecord<String, byte[]> record : records) {
                        if( record.topic().equals(subscription) !=true){
                            log.error("consumer topic = {}, subscribed topic={}", record.topic(), subscription);
}
                        callback.messageReceived(record.value(), record.timestamp());

                        offsetMap.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset()));
                    }

                    if (!offsetMap.isEmpty()) {
                        consumer.commitSync(offsetMap);
                    }
                }catch(Exception e){
                    log.error("exception occur while consuming message", e);
                }
            }
        });
    }

    @Override
    public void close() throws Exception {
        closing = true;
        executor.shutdown();
        consumerTask.get();
        consumer.close();
    }

    public boolean isClosed(){
        boolean closeStatus = closing;
        return closeStatus;    
    }
    
    public synchronized void unsubscribe() throws Exception {
        Set<String> topics = consumer.listTopics().keySet();
        //StringBuffer subs = new StringBuffer();
        //for(String topic : topics) {
        //    subs = subs.append(topic).append(",");
        //}
        //log.info("Unsub: Current subscriptions = {}", subs.toString());
        // 
        if(topics.size() > 0){
            consumer.unsubscribe();
        }  
        //log.info("Unsubscribed"); 
    }

    public synchronized void setSubscription(String topic) {
        log.info("prev= {}, cur= {} " , subscription , topic);
        subscription = topic; 
        isSubscriptionChanged.set(true);
    }
    public synchronized void subscribe() throws Exception {
        //log.info("subscribing to topic {} " , subscription);
        consumer.subscribe(Arrays.asList(subscription));
        //log.info("subscribed");
        //Set<String> topics = consumer.listTopics().keySet();
        //StringBuffer subs = new StringBuffer();
        //for(String topic : topics) {
        //    subs = subs.append(topic).append(",");
        //}
        //log.info("SUb: Current subscriptions = {}", subs.toString()); 

        isSubscriptionChanged.set(false);
    }

    public synchronized double getAverageSubscriptionChangeTime(){
        return subscriptionChangeTime
            .stream()
            .mapToDouble(a->a)
            .average()
            .orElse(0.0D);
    }
}
