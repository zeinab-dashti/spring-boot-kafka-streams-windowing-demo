# Spring Boot Kafka Streams Windowing Demo

## Overview
Kafka Streams windowing is a powerful feature that enables processing data streams based on time windows.

This example include four pipelines to demonstrate the use of Kafka Streams windowing with the Kafka Streams DSL. Each pipeline demonstrate one of window types.
- Hopping Window
- Tumbling Window
- Sliding Window
- Session Window


## Prerequisites
* Java 17 or higher
* Maven
* Docker (optional, for running Docker Compose which include Zookeeper and Apache Kafka)


## Running the Application
### Using Docker Compose
1. **Start Kafka and Zookeeper**:
   ```sh
   docker-compose up
   ```

2. **Build and Run the Application**:
   ```sh
   mvn clean package spring-boot:run
   ```


After starting the application, first the required topics are created and then some events are produced by [producer](./src/main/java/space/zeinab/demo/windowing/EventProducer.java) on the input topic.
Then the four different pipelines in this example's `streamWindows` package process these events separately. Each pipeline includes different windowing techniques to window the data stream and aggregate the data and finally produce the result to the output topic.
Final messages will also be printed on the screen.
