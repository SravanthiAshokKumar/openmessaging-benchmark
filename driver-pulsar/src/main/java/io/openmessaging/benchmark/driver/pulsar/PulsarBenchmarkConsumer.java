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
package io.openmessaging.benchmark.driver.pulsar;

import org.apache.pulsar.client.api.Consumer;

import io.openmessaging.benchmark.driver.BenchmarkConsumer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

public class PulsarBenchmarkConsumer implements BenchmarkConsumer {

    private Consumer<byte[]> consumer;
    private Consumer<byte[]> expiredConsumer;
    private AtomicBoolean isUnsubscribing = new AtomicBoolean();
    //private ExecutorService executor;
    private ReentrantLock lock = new ReentrantLock();
    
    public PulsarBenchmarkConsumer(Consumer<byte[]> consumer) {
        this.consumer = consumer;
        isUnsubscribing.set(false);
       // executor = Executors.newSingleThreadExecutor();
    }

    public void setConsumer(Consumer<byte[]> consumer) {
       /* try{
            lock.lock();
            this.consumer = consumer;
        } finally{
            lock.unlock();
        }*/
        this.consumer = consumer;
    }
    @Override
    public void close() throws Exception {
        //executor.shutdown();
        consumer.close();
    }

    public String getSubscription() {
        return consumer.getSubscription();
    }
    
    /*public class UnsubscribeTask implements Runnable{
        public void run()  {
            try{
                lock.lock();
                expiredConsumer = consumer;
            }
            finally{
                lock.unlock();
            }
            try {
                expiredConsumer.unsubscribe();
                expiredConsumer.close();
            } catch(Exception ex){
                
            }
        } 
    }*/
    
    public void unsubscribe() {
       // UnsubscribeTask task = new UnsubscribeTask();
       // executor.execute(task);
        consumer.unsubscribeAsync();
    }
    public String getTopic(){
        return consumer.getTopic();
    }    
}
