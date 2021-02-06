package io.openmessaging.benchmark;

import epl.pubsub.location.indexperf.Index;
import epl.pubsub.location.indexperf.IndexFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Scanner;

import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.TopicSubscription;
import io.openmessaging.benchmark.worker.Worker;
import io.openmessaging.benchmark.worker.IndexConfig;
import io.openmessaging.benchmark.utils.PaddingDecimalFormat;
import io.openmessaging.benchmark.utils.Timer;
import io.openmessaging.benchmark.utils.payload.FilePayloadReader;
import io.openmessaging.benchmark.utils.payload.PayloadReader;

import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkloadGeneratorWithLocations implements WorkloadGeneratorInterface {

    private final String driverName;
    private final Workload workload;
    private final Worker worker;
    private final String locations;

    private Map<Double, List<Triplet<String, Double, Double>>> timeToTuple = new LinkedHashMap<>();
    private Map<String, Boolean> clientExists = new HashMap<>();
    
    private volatile boolean runCompleted = false;
    private volatile boolean needToWaitForBacklogDraining = false;

    private volatile double targetPublishRate;

    private Index index;
    private IndexConfig indexConfig;

    public WorkloadGeneratorWithLocations(String driverName, Workload workload,
        Worker worker, String locations) {
        this.driverName = driverName;
        this.workload = workload;
        this.worker = worker;
        this.locations = locations;
        log.info("driver name = " + driverName);
        if(workload instanceof MovingWorkload){
            log.info("got moving workload");
        }
        else {
            log.info("got static workload");
        }
        if (workload.consumerBacklogSizeGB > 0 && workload.producerRate == 0) {
            throw new IllegalArgumentException("Cannot probe producer sustainable rate when building backlog");
        }
    }

    public TestResult run() throws Exception {
        Timer timer = new Timer();
        List<String> topics;
  
        // Integrate GEOHASH here
        this.indexConfig = workload.indexConfig;

        Properties props = new Properties();
        if (indexConfig.indexType.equals("GEOHASH")) {
            index =
            IndexFactory.getInitializedIndex(
                indexConfig.minX,
                indexConfig.minY,
                indexConfig.maxX,
                indexConfig.maxY,
                indexConfig.blockSize,
                IndexFactory.IndexType.GEOHASH,
                props
            );
        } else {
        index =
                IndexFactory.getInitializedIndex(
                indexConfig.minX,
                indexConfig.minY,
                indexConfig.maxX,
                indexConfig.maxY,
                indexConfig.blockSize,
                IndexFactory.IndexType.RTREE,
                props
            );
        }

        log.info("created index");
        parseLocations();

        final PayloadReader payloadReader = new FilePayloadReader(workload.messageSize);
        
        byte[] payloadData = payloadReader.load(workload.payloadFile);

        Runnable readInput = () -> {
            Iterator<Entry<Double, List<Triplet<String, Double, Double>>>> it = 
                timeToTuple.entrySet().iterator();
            Double keyToRemove = null;
            while (it.hasNext()) {
                List<Triplet<String, Double, Double>> value = it.next().getValue();
                for (int i = 0; i < value.size(); i++) {
                    Triplet<String, Double, Double> tuple = value.get(i);
                    String clientID = tuple.getValue0();
                    
                    // get topics based on the location
                    List<String> consumerTopics = index.getNearestNeighbors(tuple.getValue1(),
                        tuple.getValue2());
                    String producerTopic = index.getStringValue(tuple.getValue1(),
                        tuple.getValue2());
                    // create consumer/producer and subscribe/publish
                    try {
                        createProducer(producerTopic, clientID, payloadData);
                        if (consumerTopics != null) {
                            createConsumer(consumerTopics, clientID);
                        }
                    } catch (IOException ex) {
                        log.warn("Error while creating producer/consumer, {}", ex);
                    }
                }
                it.remove();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        };

        ExecutorService executor = Executors.newCachedThreadPool();
        executor.execute(readInput);


        log.info("----- Starting warm-up traffic ------");

        printAndCollectStats(1, TimeUnit.MINUTES);

        worker.resetStats();
        log.info("----- Starting benchmark traffic ------");

        TestResult result = printAndCollectStats(workload.testDurationMinutes, TimeUnit.MINUTES);
        runCompleted = true;

        worker.stopAll();
        executor.shutdownNow();
        return result;
    }

    @Override
    public void close() throws Exception {
        worker.stopAll();
    }

    private void parseLocations() {
        File loc = new File(locations);

        try{
            Scanner locReader = new Scanner(loc);
            while (locReader.hasNextLine()) {
                String line = locReader.nextLine();
                String[] fields = line.split("\\s+");

                double time = Double.parseDouble(fields[3]);
                if (timeToTuple.containsKey(time)) {
                    timeToTuple.get(time).add(new Triplet(fields[0],
                        Double.parseDouble(fields[1]),
                        Double.parseDouble(fields[2])));
                } else {
                    List<Triplet<String, Double, Double>> insertVal = new ArrayList();
                    insertVal.add(new Triplet(fields[0],
                        Double.parseDouble(fields[1]), Double.parseDouble(fields[2])));
                    timeToTuple.put(time, insertVal);
                }
            }
        } catch (FileNotFoundException ex) {
            log.warn("Failure in opening the given file", ex);
        }
    }

    private ConsumerAssignment createConsumerAssignment(List<String> topics, String consumerID){
        ConsumerAssignment consumerAssignment = new ConsumerAssignment();

        String subscriptionName = String.format("sub-%s", consumerID);
        for(String topic: topics){
            consumerAssignment.topicsSubscriptions
                .add(new TopicSubscription(topic, subscriptionName));
        }
        return consumerAssignment;
    }
    
    private void createConsumer(List<String> topics, String consumerID) throws IOException {
        ConsumerAssignment consumerAssignment = createConsumerAssignment(topics, consumerID);
        Timer timer = new Timer();

        worker.createConsumers(consumerAssignment);
        log.info("Created consumer in {} ms", timer.elapsedMillis());
    }

    private void createProducer(String topic, String producerID, byte[] payloadData)
        throws IOException {
        Timer timer = new Timer();

        worker.createProducers(topic, producerID, payloadData);
        log.info("Created a producer in {} ms", timer.elapsedMillis());
    }

    private TestResult printAndCollectStats(long testDurations, TimeUnit unit) 
        throws IOException {
        
        long startTime = System.nanoTime();

        // Print report stats
        long oldTime = System.nanoTime();

        long testEndTime = testDurations > 0 ? startTime + unit.toNanos(testDurations) : Long.MAX_VALUE;

        TestResult result = new TestResult();
        result.workload = workload.name;
        result.driver = driverName;

        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                break;
            }
            PeriodStats stats = worker.getPeriodStats();

            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;

            double publishRate = stats.messagesSent / elapsed;
            double publishThroughput = stats.bytesSent / elapsed / 1024 / 1024;

            double consumeRate = stats.messagesReceived / elapsed;
            double consumeThroughput = stats.bytesReceived / elapsed / 1024 / 1024;

            long currentBacklog = workload.subscriptionsPerTopic * stats.totalMessagesSent
                    - stats.totalMessagesReceived;

            log.info(
                    "Pub rate {} msg/s / {} Mb/s | Cons rate {} msg/s / {} Mb/s | Backlog: {} K | Pub Latency (ms) avg: {} - 50%: {} - 99%: {} - 99.9%: {} - Max: {}",
                    rateFormat.format(publishRate), throughputFormat.format(publishThroughput),
                    rateFormat.format(consumeRate), throughputFormat.format(consumeThroughput),
                    dec.format(currentBacklog / 1000.0), //
                    dec.format(microsToMillis(stats.publishLatency.getMean())),
                    dec.format(microsToMillis(stats.publishLatency.getValueAtPercentile(50))),
                    dec.format(microsToMillis(stats.publishLatency.getValueAtPercentile(99))),
                    dec.format(microsToMillis(stats.publishLatency.getValueAtPercentile(99.9))),
                    throughputFormat.format(microsToMillis(stats.publishLatency.getMaxValue())));

            result.messagesSent = stats.messagesSent;
            result.messagesReceived = stats.messagesReceived;
            result.publishRate.add(publishRate);
            result.consumeRate.add(consumeRate);
            result.backlog.add(currentBacklog);
            result.publishLatencyAvg.add(microsToMillis(stats.publishLatency.getMean()));
            result.publishLatency50pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(50)));
            result.publishLatency75pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(75)));
            result.publishLatency95pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(95)));
            result.publishLatency99pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(99)));
            result.publishLatency999pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(99.9)));
            result.publishLatency9999pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(99.99)));
            result.publishLatencyMax.add(microsToMillis(stats.publishLatency.getMaxValue()));

            result.endToEndLatencyAvg.add(microsToMillis(stats.endToEndLatency.getMean()));
            result.endToEndLatency50pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(50)));
            result.endToEndLatency75pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(75)));
            result.endToEndLatency95pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(95)));
            result.endToEndLatency99pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(99)));
            result.endToEndLatency999pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(99.9)));
            result.endToEndLatency9999pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(99.99)));
            result.endToEndLatencyMax.add(microsToMillis(stats.endToEndLatency.getMaxValue()));

            result.subscriptionChangeLatencyAvg.add(microsToMillis(stats.subscriptionChangeLatency.getMean()));
            result.subscriptionChangeLatency50pct.add(microsToMillis(stats.subscriptionChangeLatency.getValueAtPercentile(50)));
            result.subscriptionChangeLatency75pct.add(microsToMillis(stats.subscriptionChangeLatency.getValueAtPercentile(75)));
            result.subscriptionChangeLatency95pct.add(microsToMillis(stats.subscriptionChangeLatency.getValueAtPercentile(95)));
            result.subscriptionChangeLatency99pct.add(microsToMillis(stats.subscriptionChangeLatency.getValueAtPercentile(99)));
            result.subscriptionChangeLatencyMax.add(microsToMillis(stats.subscriptionChangeLatency.getMaxValue()));

            if (now >= testEndTime && !needToWaitForBacklogDraining) {
                CumulativeLatencies agg = worker.getCumulativeLatencies();
                log.info(
                        "----- Aggregated Pub Latency (ms) avg: {} - 50%: {} - 95%: {} - 99%: {} - 99.9%: {} - 99.99%: {} - Max: {}",
                        dec.format(agg.publishLatency.getMean() / 1000.0),
                        dec.format(agg.publishLatency.getValueAtPercentile(50) / 1000.0),
                        dec.format(agg.publishLatency.getValueAtPercentile(95) / 1000.0),
                        dec.format(agg.publishLatency.getValueAtPercentile(99) / 1000.0),
                        dec.format(agg.publishLatency.getValueAtPercentile(99.9) / 1000.0),
                        dec.format(agg.publishLatency.getValueAtPercentile(99.99) / 1000.0),
                        throughputFormat.format(agg.publishLatency.getMaxValue() / 1000.0));

                result.aggregatedPublishLatencyAvg = agg.publishLatency.getMean() / 1000.0;
                result.aggregatedPublishLatency50pct = agg.publishLatency.getValueAtPercentile(50) / 1000.0;
                result.aggregatedPublishLatency75pct = agg.publishLatency.getValueAtPercentile(75) / 1000.0;
                result.aggregatedPublishLatency95pct = agg.publishLatency.getValueAtPercentile(95) / 1000.0;
                result.aggregatedPublishLatency99pct = agg.publishLatency.getValueAtPercentile(99) / 1000.0;
                result.aggregatedPublishLatency999pct = agg.publishLatency.getValueAtPercentile(99.9) / 1000.0;
                result.aggregatedPublishLatency9999pct = agg.publishLatency.getValueAtPercentile(99.99) / 1000.0;
                result.aggregatedPublishLatencyMax = agg.publishLatency.getMaxValue() / 1000.0;

                result.aggregatedEndToEndLatencyAvg = agg.endToEndLatency.getMean()  / 1000.0;
                result.aggregatedEndToEndLatency50pct = agg.endToEndLatency.getValueAtPercentile(50)  / 1000.0;
                result.aggregatedEndToEndLatency75pct = agg.endToEndLatency.getValueAtPercentile(75)  / 1000.0;
                result.aggregatedEndToEndLatency95pct = agg.endToEndLatency.getValueAtPercentile(95)  / 1000.0;
                result.aggregatedEndToEndLatency99pct = agg.endToEndLatency.getValueAtPercentile(99)  / 1000.0;
                result.aggregatedEndToEndLatency999pct = agg.endToEndLatency.getValueAtPercentile(99.9)  / 1000.0;
                result.aggregatedEndToEndLatency9999pct = agg.endToEndLatency.getValueAtPercentile(99.99)  / 1000.0;
                result.aggregatedEndToEndLatencyMax = agg.endToEndLatency.getMaxValue()  / 1000.0;

                result.aggregatedsubscriptionChangeLatencyAvg = agg.subscriptionChangeLatency.getMean()  / 1000.0;
                result.aggregatedsubscriptionChangeLatency50pct = agg.subscriptionChangeLatency.getValueAtPercentile(50)  / 1000.0;
                result.aggregatedsubscriptionChangeLatency75pct = agg.subscriptionChangeLatency.getValueAtPercentile(75)  / 1000.0;
                result.aggregatedsubscriptionChangeLatency95pct = agg.subscriptionChangeLatency.getValueAtPercentile(95)  / 1000.0;
                result.aggregatedsubscriptionChangeLatency99pct = agg.subscriptionChangeLatency.getValueAtPercentile(99)  / 1000.0;
                result.aggregatedsubscriptionChangeLatencyMax = agg.subscriptionChangeLatency.getMaxValue()  / 1000.0;

                agg.publishLatency.percentiles(100).forEach(value -> {
                    result.aggregatedPublishLatencyQuantiles.put(value.getPercentile(),
                            value.getValueIteratedTo() / 1000.0);
                });

                agg.endToEndLatency.percentiles(100).forEach(value -> {
                    result.aggregatedEndToEndLatencyQuantiles.put(value.getPercentile(),
                            microsToMillis(value.getValueIteratedTo()));
                });
                
    
                break;
            }
                
            oldTime = now;
        }

        return result;
    }

    private static final DecimalFormat rateFormat = new PaddingDecimalFormat("0.0", 7);
    private static final DecimalFormat throughputFormat = new PaddingDecimalFormat("0.0", 4);
    private static final DecimalFormat dec = new PaddingDecimalFormat("0.0", 4);

    private static double microsToMillis(double timeInMicros) {
        return timeInMicros / 1000.0;
    }

    private static double microsToMillis(long timeInMicros) {
        return timeInMicros / 1000.0;
    }

    private static final Logger log = LoggerFactory.getLogger(WorkloadGenerator.class);
}