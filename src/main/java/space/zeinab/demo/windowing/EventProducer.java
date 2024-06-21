package space.zeinab.demo.windowing;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import space.zeinab.demo.windowing.config.KafkaConfig;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

@Slf4j
@Component
public class EventProducer {

    @Async
    @PostConstruct
    public void produceEvents() {
        Producer<String, String> producer = new KafkaProducer<>(KafkaConfig.getProducerProperties());

        // Define the list of time intervals in seconds
        List<Long> timestamps = Arrays.asList(
                1000L, // 1 second
                3000L, // 2 seconds after previous
                8000L, // 5 seconds after previous
                10000L, // 2 seconds after previous
                18000L, // 8 seconds after previous
                18000L, // 10 seconds after previous
                29000L, // 11 seconds after previous
                45000L // 16 seconds after previous
        );

        // Define the base time
        long baseTime = System.currentTimeMillis();

        try {
            int eventCounter = 0;
            for (long relativeTime : timestamps) {
                String key = "user" + (eventCounter % 3);  // Three users: user0, user1, user2
                String value = "login";
                long timestamp = baseTime + relativeTime;

                // Create an event with the specific timestamp
                ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConfig.WINDOW_INPUT_TOPIC, 0, timestamp, key, value);

                producer.send(record, (recordMetadata, exception) -> {
                    if (exception == null) {
                        Instant instant = Instant.ofEpochMilli(recordMetadata.timestamp());
                        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
                        System.out.println("Produced event: " + key + " - " + value + " at " + localDateTime.format(DateTimeFormatter.ISO_DATE_TIME));
                    } else {
                        log.error(exception.toString());
                    }
                });

                // Increment the event counter
                eventCounter++;
            }
        } finally {
            producer.close();
        }
    }
}