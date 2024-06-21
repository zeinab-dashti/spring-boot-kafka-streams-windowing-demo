package space.zeinab.demo.windowing.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Configuration
@EnableKafka
@EnableKafkaStreams
@Slf4j
public class KafkaConfig {
    public static final String WINDOW_INPUT_TOPIC = "window-input-topic";
    public static final String WINDOW_OUTPUT_TOPIC = "window-output-topic";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = getAdminProperties();
        return new KafkaAdmin(configs);
    }

    @Bean
    public Collection<NewTopic> createTopics(KafkaAdmin kafkaAdmin) {
        log.info("Starting the topics creation");

        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            HashMap<String, NewTopic> topics = new HashMap<>();
            topics.put(WINDOW_INPUT_TOPIC, new NewTopic(WINDOW_INPUT_TOPIC, 1, (short) 1));
            topics.put(WINDOW_OUTPUT_TOPIC, new NewTopic(WINDOW_OUTPUT_TOPIC, 1, (short) 1));


            CreateTopicsResult result = adminClient.createTopics(topics.values());
            result.values().forEach((topicName, future) -> {
                NewTopic topic = topics.get(topicName);
                future.whenComplete((aVoid, maybeError) ->
                        {
                            if (maybeError != null) {
                                log.error("Topic creation didn't complete:", maybeError);
                            } else {
                                log.info(
                                        String.format(
                                                "Topic %s, has been successfully created " +
                                                        "with %s partitions and replicated %s times",
                                                topic.name(),
                                                topic.numPartitions(),
                                                topic.replicationFactor() - 1
                                        )
                                );
                            }
                        }
                );
            });
            result.all().get();

            return topics.values();
        } catch (InterruptedException | ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                log.error("Topic creation failed", e);
            }
        }
        return List.of(); // Return an empty list if topic creation fails
    }

    public static Map<String, Object> getAdminProperties() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        return configs;
    }

    public static Properties getStreamProperties(String applicationId, String relativePath) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        String stateDir = System.getProperty("user.dir") + "/" + relativePath;
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        return props;
    }

    public static Properties getProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return props;
    }
}