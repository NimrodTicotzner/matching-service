package com.redice.matching.topology;

import com.redice.matching.services.KeysGenerator;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;

@Service
public class MatchTopologyExecutorImpl implements MatchTopologyExecutor {

    private StreamsBuilder streamsBuilder;
    private KafkaStreams kstreams;
    private Properties streamsConfiguration;

    @Value("${bootstrap.servers}")
    private String bootstrapServers;

    @Value("${match.topic.out}")
    private String matchTopicOut;

    @Autowired
    private KeysGenerator keysGenerator;

    private Logger logger = LoggerFactory.getLogger(getClass());

    @PreDestroy
    public void close() {
        if (kstreams != null) {
            kstreams.close();
            kstreams.cleanUp();
        }
    }

    @Override
    public String deployAfMatchTopology(String appName, String matchTopicX, String matchTopicY, int expectedMsgsCount) throws ExecutionException, InterruptedException {
        /*ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Boolean> future = executor.submit(new Task(bootstrapServers, matchTopicOut, expectedMsgsCount, 60, logger));*/

        logger.info("Starting match process");
        logger.info(matchTopicX + ":" + matchTopicY);

        streamsConfiguration = initStreamsConfig(appName);
        streamsBuilder = new StreamsBuilder();

        KStream<String, String> matchStreamX = streamsBuilder.stream(matchTopicX, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> keyTransformedStreamX = matchStreamX.selectKey((k, v) -> keysGenerator.composeHashedKey(v));

        KStream<String, String> matchStreamY = streamsBuilder.stream(matchTopicY, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> keyTransformedStreamY = matchStreamY.selectKey((k, v) -> keysGenerator.composeHashedKey(v));

        KStream<String, String> joined = keyTransformedStreamX.leftJoin(keyTransformedStreamY,
                this::initAfMatchResponse,
                JoinWindows.of(20 * 1000),
                Joined.with(
                        Serdes.String(),
                        Serdes.String(),
                        Serdes.String()
                ));

        joined.to(matchTopicOut, Produced.with(Serdes.String(), Serdes.String()));

        kstreams = new KafkaStreams(streamsBuilder.build(), streamsConfiguration);
        kstreams.cleanUp();
        kstreams.start();

        //Boolean res = future.get();

        /*kstreams.close();
        kstreams.cleanUp();*/
        //return res.toString();
        return "messi";
    }

    private String initAfMatchResponse(String v1, String v2) {
        if (v2 == null) {
            return "no-match";
        }
        return "match";
    }

    private Properties initStreamsConfig(String appName) {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5 * 1000);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "./state");
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 20 * 1024 * 1024L);
        return streamsConfiguration;
    }

}

class Task implements Callable<Boolean> {
    private String bootstrapServers;
    private String matchTopicOut;
    private int expectedCount;
    private Logger logger;
    private int durationInSecs;

    Task(String bootstrapServers, String matchTopicOut, int expectedCount, int durationInSecs, Logger logger) {
        this.bootstrapServers = bootstrapServers;
        this.matchTopicOut = matchTopicOut;
        this.expectedCount = expectedCount;
        this.durationInSecs = durationInSecs;
        this.logger = logger;
    }

    @Override
    public Boolean call() {
        logger.info("Starting matches counting thread.");
        boolean endMatching = false;
        boolean match = true;
        int msgCount = 0;
        long endTime = System.currentTimeMillis() + (durationInSecs * 1000);

        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "match-reporter");
        final Consumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(matchTopicOut));

        while (!endMatching && System.currentTimeMillis() <= endTime) {
            final ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                msgCount++;
                if (record.value().equals("no-match")) {
                    endMatching = true;
                    match = false;
                }
                if (msgCount == expectedCount) {
                    endMatching = true;
                }
            }

            consumer.commitAsync();
        }

        consumer.close();
        return match;
    }
}
