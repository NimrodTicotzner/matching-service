package com.redice.matching.topology;

import com.redice.matching.entities.MatchRequest;
import com.redice.matching.entities.MatchRequestJoiner;
import com.redice.matching.entities.MatchResponse;
import com.redice.matching.serdes.JsonPOJODeserializer;
import com.redice.matching.serdes.JsonPOJOSerializer;
import com.redice.matching.services.KeysGenerator;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Service
public class MatchTopologyExecutorImpl implements MatchTopologyExecutor {

    private StreamsBuilder streamsBuilder;
    private KafkaStreams kstreams;
    private Properties streamsConfiguration;
    private Serde<MatchRequest> matchRequestSerde;
    private Serde<MatchResponse> matchResponseSerde;

    @Value("${bootstrap.servers}")
    private String bootstrapServers;

    @Value("${match.topic.x}")
    private String matchTopicX;

    @Value("${match.topic.y}")
    private String matchTopicY;

    @Value("${match.topic.out}")
    private String matchTopicOut;

    @Autowired
    private KeysGenerator keysGenerator;

    private static final String KEY_DELIMITER = "#";

    private Logger logger = LoggerFactory.getLogger(getClass());

    @PostConstruct
    public void init() {
        prepareSerializers();
    }

    @PreDestroy
    public void close() {
        if (kstreams != null) {
            kstreams.close();
            kstreams.cleanUp();
        }
    }

    @Override
    public void deployMatchTopology(String appName, MatchRequestJoiner matchRequestJoiner) {
        streamsConfiguration = initStreamsConfig(appName);
        streamsBuilder = new StreamsBuilder();

        KStream<String, MatchRequest> matchStreamX = streamsBuilder.stream(matchTopicX, Consumed.with(Serdes.String(), matchRequestSerde));
        KStream<String, MatchRequest> keyTransformedMatchStreamX = matchStreamX.selectKey((k,v) -> keysGenerator.composeComplexKey(matchRequestJoiner, v));

        KStream<String, MatchRequest> matchStreamY = streamsBuilder.stream(matchTopicY, Consumed.with(Serdes.String(), matchRequestSerde));
        KStream<String, MatchRequest> keyTransformedMatchStreamY = matchStreamY.selectKey((k,v) -> keysGenerator.composeComplexKey(matchRequestJoiner, v));

        KStream<String, MatchResponse> joined = keyTransformedMatchStreamX.join(keyTransformedMatchStreamY,
                this::initMatchResponse,
                JoinWindows.of(matchRequestJoiner.getJoinTime()),
                Joined.with(
                        Serdes.String(),
                        matchRequestSerde,
                        matchRequestSerde
                ));

        joined.to(matchTopicOut, Produced.with(Serdes.String(), matchResponseSerde));

        kstreams = new KafkaStreams(streamsBuilder.build(), streamsConfiguration);
        kstreams.cleanUp();
        kstreams.start();
    }

    private MatchResponse initMatchResponse(MatchRequest l, MatchRequest r) {
        MatchResponse matchResponse = new MatchResponse();
        matchResponse.setFirstUserId(l.getUserId());
        matchResponse.setFirstUserSocketId(l.getSocketId());
        matchResponse.setFirstUserLevel(l.getLevel());

        matchResponse.setSecondUserId(r.getUserId());
        matchResponse.setSecondUserSocketId(r.getSocketId());
        matchResponse.setSecondUserLevel(r.getLevel());

        return matchResponse;
    }

    private void prepareSerializers() {
        Map<String, Object> serdeProps = new HashMap<>();

        final Serializer<MatchRequest> matchRequestSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", MatchRequest.class);
        matchRequestSerializer.configure(serdeProps, false);

        final Deserializer<MatchRequest> matchRequestDeserializer = new JsonPOJODeserializer<>();
        matchRequestDeserializer.configure(serdeProps, false);

        final Serializer<MatchResponse> matchResponseSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", MatchResponse.class);
        matchResponseSerializer.configure(serdeProps, false);

        final Deserializer<MatchResponse> matchResponseDeserializer = new JsonPOJODeserializer<>();
        matchResponseDeserializer.configure(serdeProps, false);

        matchRequestSerde = Serdes.serdeFrom(matchRequestSerializer, matchRequestDeserializer);
        matchResponseSerde = Serdes.serdeFrom(matchResponseSerializer, matchResponseDeserializer);
    }

    private Properties initStreamsConfig(String appName) {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5 * 1000);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "./state");
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 20 * 1024 * 1024L);
        return streamsConfiguration;
    }

    /*private String composeComplexKey(MatchRequestJoiner matchRequestJoiner, MatchRequest matchRequest) {
        *//*String normalizedLevel = String.valueOf(Integer.parseInt(matchRequest.getLevel()) / matchLevelDivider);
        final String key = String.join(KEY_DELIMITER, matchRequest.getCourse(), normalizedLevel);*//*


    }*/

}
