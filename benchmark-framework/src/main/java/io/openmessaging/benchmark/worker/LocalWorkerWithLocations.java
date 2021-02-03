package io.openmessaging.benchmark.worker;

import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.DriverConfiguration;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.MovingConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import io.openmessaging.benchmark.utils.Timer;
import io.openmessaging.benchmark.utils.distributor.KeyDistributor;
import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;

import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.MovingCumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.MovingPeriodStats;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.HdrHistogram.Recorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalWorkerWithLocations implements Worker, ConsumerCallback {
    
    private BenchmarkDriver benchmarkDriver = null;

    private Map<String, ScheduledExecutorService> producers = new ConcurrentHashMap<>();
    private Map<String, BenchmarkConsumer> consumers = new ConcurrentHashMap<>();

    // defining stats

    private final StatsLogger statsLogger;

    private final LongAdder messagesSent = new LongAdder();
    private final LongAdder bytesSent = new LongAdder();
    private final Counter messagesSentCounter;
    private final Counter bytesSentCounter;

    private final LongAdder messagesReceived = new LongAdder();
    private final LongAdder bytesReceived = new LongAdder();
    private final Counter messagesReceivedCounter;
    private final Counter bytesReceivedCounter;

    private final LongAdder totalMessagesSent = new LongAdder();
    private final LongAdder totalMessagesReceived = new LongAdder();

    private final Recorder publishLatencyRecorder = new Recorder(
        TimeUnit.SECONDS.toMicros(60), 5);
    private final Recorder cumulativePublishLatencyRecorder = new Recorder(
        TimeUnit.SECONDS.toMicros(60), 5);
    private final OpStatsLogger publishLatencyStats;

    private final Recorder endToEndLatencyRecorder = new Recorder(
        TimeUnit.HOURS.toMicros(12), 5);
    private final Recorder endToEndCumulativeLatencyRecorder = new Recorder(
        TimeUnit.HOURS.toMicros(12), 5);
    private final OpStatsLogger endToEndLatencyStats;


    private final Recorder subscriptionChangeLatencyRecorder = new Recorder(
        TimeUnit.SECONDS.toMicros(60), 5);
    private final Recorder subscriptionChangeCumulativeLatencyRecorder = new Recorder(
        TimeUnit.SECONDS.toMicros(60), 5);


    private boolean testCompleted = false;

    private boolean consumersArePaused = false;

    private ConsumerAssignment consumerAssignment;

    class SubscriptionChangeTask implements Runnable {
        private List<String> topics;
        private String subscription;
        private ConsumerCallback consumerCallback;

        public SubscriptionChangeTask(List<String> topics, String subscription, 
                                      ConsumerCallback consumerCallback) {
            this.topics = topics;
            this.subscription = subscription;
            this.consumerCallback = consumerCallback;
        }

        public void run(){
            try {
                log.info("Changing subs for consumers");
                changeSubscriptions();
            } catch(Exception e) {
                log.error("Could NOT change Subscriptions because {}", e.getMessage());
            }
        }
        
        public void changeSubscriptions() {
            Timer timer = new Timer();
            BenchmarkConsumer currentConsumer = consumers.get(this.subscription);
            CompletableFuture<BenchmarkConsumer> future = benchmarkDriver.createMultiTopicConsumer(
                topics, subscription, this.consumerCallback);
            try {
                currentConsumer.close();
            } catch (Exception ex) {
                log.warn("Error occured while closing the consumer connection, {}", ex);
            }
            consumers.put(this.subscription, future.join());

            double curSubTime = timer.elapsedMillis();
            log.info("cur sub time = {}", curSubTime);

            subscriptionChangeLatencyRecorder.recordValue(
                TimeUnit.MILLISECONDS.toMicros((long)curSubTime));
            subscriptionChangeCumulativeLatencyRecorder.recordValue(
                TimeUnit.MILLISECONDS.toMicros((long)curSubTime)); 
        }
    }

    class ProducerTask implements Runnable {
        private BenchmarkProducer producer;
        private String topic;
        private byte[] payloadData;
        private KeyDistributor keyDistributor;

        public ProducerTask(BenchmarkProducer producer, String topic, byte[] payloadData) {
            this.producer = producer;
            this.topic = topic;
            this.payloadData = payloadData;
            this.keyDistributor = KeyDistributor.build(KeyDistributorType.NO_KEY);
        }

        public void run(){
            try {
                log.info("running producer task for topic {}", this.topic);
                Timer timer = new Timer();
                runTask();
            } catch(Exception e) {
                log.error("Could NOT change Subscriptions because {}", e.getMessage());
            }
        }
        
        public void runTask() {
            Timer timer = new Timer();
            
            final long sendTime = System.nanoTime();
            this.producer.sendAsync(Optional.ofNullable(keyDistributor.next()), payloadData)
                    .thenRun(() -> {
                messagesSent.increment();
                totalMessagesSent.increment();
                messagesSentCounter.inc();
                bytesSent.add(payloadData.length);
                bytesSentCounter.add(payloadData.length);

                long latencyMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - sendTime);
                publishLatencyRecorder.recordValue(latencyMicros);
                cumulativePublishLatencyRecorder.recordValue(latencyMicros);
                publishLatencyStats.registerSuccessfulEvent(latencyMicros, TimeUnit.MICROSECONDS);
            }).exceptionally(ex -> {
                log.warn("Write error on message", ex);
                return null;
            });
        }
    }

    public LocalWorkerWithLocations() {
        this(NullStatsLogger.INSTANCE);
    }

    public LocalWorkerWithLocations(StatsLogger statsLogger) {
        this.statsLogger = statsLogger;

        StatsLogger producerStatsLogger = statsLogger.scope("producer");
        this.messagesSentCounter = producerStatsLogger.getCounter("messages_sent");
        this.bytesSentCounter = producerStatsLogger.getCounter("bytes_sent");
        this.publishLatencyStats = producerStatsLogger.getOpStatsLogger("produce_latency");

        StatsLogger consumerStatsLogger = statsLogger.scope("consumer");
        this.messagesReceivedCounter = consumerStatsLogger.getCounter("messages_recv");
        this.bytesReceivedCounter = consumerStatsLogger.getCounter("bytes_recv");
        this.endToEndLatencyStats = consumerStatsLogger.getOpStatsLogger("e2e_latency");
    }

    @Override
    public void initializeDriver(File driverConfigFile) throws IOException {
        Preconditions.checkArgument(benchmarkDriver == null);
        testCompleted = false;

        DriverConfiguration driverConfiguration = mapper.readValue(
            driverConfigFile, DriverConfiguration.class);

        log.info("Driver: {}", writer.writeValueAsString(driverConfiguration));

        try {
            benchmarkDriver = (BenchmarkDriver) Class.forName(driverConfiguration.driverClass).newInstance();
            benchmarkDriver.initialize(driverConfigFile, statsLogger);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> createTopics(TopicsInfo topicsInfo) {
        return new ArrayList<String>();
    }

    @Override
    public void createProducers(List<String> topics) {
    }

    @Override
    public void createProducers(String topic, String producerID, byte[] payloadData) {
        
        Timer timer = new Timer();
        ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(5);
        CompletableFuture<BenchmarkProducer> future = benchmarkDriver.createProducer(topic);
        BenchmarkProducer producer = future.join();
        ProducerTask producerTask = new ProducerTask(producer, topic, payloadData);

        if (producers.containsKey(producerID)) {
            ScheduledExecutorService currentExecutor = producers.get(producerID);
            currentExecutor.shutdownNow();
        }

        scheduledExecutor.scheduleAtFixedRate(producerTask, 0, 1, TimeUnit.SECONDS);
        producers.put(producerID, scheduledExecutor);

        log.info("Created producer in {} ms, topic {}", timer.elapsedMillis(), topic);
    }

    @Override
    public void createConsumers(ConsumerAssignment consumerAssignment) {

        Timer timer = new Timer();
        String subscription = consumerAssignment.topicsSubscriptions.get(0).subscription;
        List<String> topics = consumerAssignment.topicsSubscriptions.stream()
            .map(ts -> ts.topic).collect(toList());
        
        if (consumers.containsKey(subscription)) {
            ExecutorService subChangeExecutor = Executors.newSingleThreadScheduledExecutor();
            SubscriptionChangeTask subscriptionChangeTask = 
                new SubscriptionChangeTask(topics, subscription, this); 
            subChangeExecutor.execute(subscriptionChangeTask);
        } else {
            CompletableFuture<BenchmarkConsumer> future = benchmarkDriver
                .createMultiTopicConsumer(topics, subscription, this);
            consumers.put(subscription, future.join());
        }

        log.info("Created MultiTopicConsumer in {} ms", timer.elapsedMillis());
    }

    @Override
    public void startLoad(ProducerWorkAssignment producerWorkAssignment) {
    }

    @Override
    public void probeProducers() throws IOException {
    }

    @Override
    public PeriodStats getPeriodStats() {
        PeriodStats stats = new PeriodStats();
        stats.messagesSent = messagesSent.sumThenReset();
        stats.bytesSent = bytesSent.sumThenReset();

        stats.messagesReceived = messagesReceived.sumThenReset();
        stats.bytesReceived = bytesReceived.sumThenReset();

        stats.totalMessagesSent = totalMessagesSent.sum();
        stats.totalMessagesReceived = totalMessagesReceived.sum();

        stats.publishLatency = publishLatencyRecorder.getIntervalHistogram();
        stats.endToEndLatency = endToEndLatencyRecorder.getIntervalHistogram();
        
        stats.subscriptionChangeLatency = 
            subscriptionChangeLatencyRecorder.getIntervalHistogram();
        
        return stats;
    }

    @Override
    public CumulativeLatencies getCumulativeLatencies() {
        CumulativeLatencies latencies = new CumulativeLatencies();
        latencies.publishLatency = cumulativePublishLatencyRecorder.getIntervalHistogram();
        latencies.endToEndLatency = endToEndCumulativeLatencyRecorder.getIntervalHistogram();
        latencies.subscriptionChangeLatency = 
            subscriptionChangeCumulativeLatencyRecorder.getIntervalHistogram();
        return latencies;
    }

    @Override
    public CountersStats getCountersStats() throws IOException {
        CountersStats stats = new CountersStats();
        stats.messagesSent = totalMessagesSent.sum();
        stats.messagesReceived = totalMessagesReceived.sum();
        return stats;
    }

    @Override
    public void messageReceived(byte[] data, long publishTimestamp) {
        // TODO: 
        messagesReceived.increment();
        totalMessagesReceived.increment();
        messagesReceivedCounter.inc();
        bytesReceived.add(data.length);
        bytesReceivedCounter.add(data.length);

        long now = System.currentTimeMillis();
        long endToEndLatencyMicros = TimeUnit.MILLISECONDS.toMicros(now - publishTimestamp);
        if (endToEndLatencyMicros > 0) {
            endToEndCumulativeLatencyRecorder.recordValue(endToEndLatencyMicros);
            endToEndLatencyRecorder.recordValue(endToEndLatencyMicros);
            endToEndLatencyStats.registerSuccessfulEvent(endToEndLatencyMicros, TimeUnit.MICROSECONDS);
        }

        while (consumersArePaused) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void pauseConsumers() throws IOException {
        consumersArePaused = true;
        log.info("Pausing consumers");
    }

    @Override
    public void resumeConsumers() throws IOException {
        consumersArePaused = false;
        log.info("Resuming consumers");
    }

    @Override
    public void resetStats() throws IOException {
        publishLatencyRecorder.reset();
        cumulativePublishLatencyRecorder.reset();
        endToEndLatencyRecorder.reset();
        endToEndCumulativeLatencyRecorder.reset();
        subscriptionChangeLatencyRecorder.reset();
    }

    @Override
    public void stopAll() throws IOException {
        testCompleted = true;
        consumersArePaused = false;

        publishLatencyRecorder.reset();
        cumulativePublishLatencyRecorder.reset();
        endToEndLatencyRecorder.reset();
        endToEndCumulativeLatencyRecorder.reset();
        subscriptionChangeLatencyRecorder.reset();

        messagesSent.reset();
        bytesSent.reset();
        messagesReceived.reset();
        bytesReceived.reset();
        totalMessagesSent.reset();
        totalMessagesReceived.reset();

        try {
            Thread.sleep(100);

            producers.forEach((k, v) -> v.shutdownNow());
            producers.clear();

            consumers.forEach((k, v) -> { 
                try{
                    v.close();
                } catch (Exception ex) {
                    log.warn("Error occured while closing the consumer connection, {}", ex);
                }
            });
            consumers.clear();

            if (benchmarkDriver != null) {
                benchmarkDriver.close();
                
                benchmarkDriver = null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        // TODO: shutdown executors if required
    }

    @Override
    public void changeConsumerSubscriptions(ConsumerAssignment consumerAssignment)
        throws IOException{
        
    }

    @Override
    public void adjustPublishRate(double publishRate) {
    }

    private static final ObjectWriter writer = new ObjectMapper()
        .writerWithDefaultPrettyPrinter();

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static {
        mapper.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);
    }

    private static final Logger log = LoggerFactory.getLogger(LocalWorker.class);
}