package io.openmessaging.benchmark.driver.kafka;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;


import io.openmessaging.benchmark.driver.BenchmarkStream;
import io.openmessaging.benchmark.driver.StreamTransform;
import io.openmessaging.benchmark.driver.StreamPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.time.StopWatch;

import java.util.Properties;
import java.util.List;
import java.util.Map;

public class KafkaBenchmarkStream implements BenchmarkStream {
    private final static Logger log = LoggerFactory.getLogger(KafkaBenchmarkStream.class);
    
    private KafkaStreams streams;
    public KafkaBenchmarkStream(Properties streamConf, String inputTopic, Map<String, StreamPredicate> topicRouting, StreamTransform transform){
        log.info("input topic {}", inputTopic);
        final Serde<String> stringSerde = Serdes.String();
        final Serde<byte[]> byteArraySerde = Serdes.ByteArray();

        final StreamsBuilder builder = new StreamsBuilder();
    
        final KStream<byte[], String> inputStream = builder.stream(inputTopic, Consumed.with(byteArraySerde, stringSerde));

        final KStream<byte[], String> transformedStream = inputStream.map((key, value) -> new KeyValue<>(key, transform.applyTransform(value)));
       
        for(String topic: topicRouting.keySet()){
            transformedStream.filter((key, value)-> topicRouting.get(topic).applyPredicate(value)).to(topic, Produced.with(byteArraySerde, stringSerde));
        } 
        streams = new KafkaStreams(builder.build(), streamConf);
        streams.cleanUp();
        streams.start();
    }
    
    @Override
    public void close(){
        streams.close();
    }
}
