package space.zeinab.demo.windowing.streamWindows;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import space.zeinab.demo.windowing.config.KafkaConfig;

import java.time.Duration;

@Component
public class HoppingWindow {

    @Autowired
    public static void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> eventStream = streamsBuilder.stream(KafkaConfig.WINDOW_INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        eventStream
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(15), Duration.ofSeconds(5)).advanceBy(Duration.ofSeconds(10)))
                .count(Materialized.as("hopping-store"))
                .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
                .toStream()
                .map((key, value) -> KeyValue.pair(
                        key.key(),
                        "Key = " + key.key() + ", Hopping Window Count = " + value
                ))
                .to(KafkaConfig.WINDOW_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), KafkaConfig.getStreamProperties("hopping-stream-app", "state/hopping-store-dir"));

        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}