package space.zeinab.demo.windowing.streamWindows;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import space.zeinab.demo.windowing.config.KafkaConfig;

import java.time.Duration;

@Component
public class SessionWindow {

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> eventStream = streamsBuilder.stream(KafkaConfig.WINDOW_INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        eventStream
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(15)))
                .count(Materialized.as("session-store"))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((key, value) -> KeyValue.pair(
                        key.key(),
                        "Key = " + key.key() + ", Session Window Count = " + value
                ))
                .to(KafkaConfig.WINDOW_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), KafkaConfig.getStreamProperties("session-stream-app", "state/session-store-dir"));

        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
