package space.zeinab.demo.windowing.streamWindows;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import space.zeinab.demo.windowing.config.KafkaConfig;

import java.time.Duration;

@Component
public class TumblingWindow {

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> eventStream = streamsBuilder.stream(KafkaConfig.WINDOW_INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        eventStream
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(15)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("tumbling-store"))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((key, value) -> KeyValue.pair(
                        key.key(),
                        "Key = " + key.key() + ", Tumbling Window Count = " + value
                ))
                .to(KafkaConfig.WINDOW_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), KafkaConfig.getStreamProperties("tumbling-stream-app", "state/tumbling-store-dir"));

        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}