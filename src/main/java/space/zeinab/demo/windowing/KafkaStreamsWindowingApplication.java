package space.zeinab.demo.windowing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;


@SpringBootApplication
@EnableAsync
public class KafkaStreamsWindowingApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsWindowingApplication.class, args);
    }
}