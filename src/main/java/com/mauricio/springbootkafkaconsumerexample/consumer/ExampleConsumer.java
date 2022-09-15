package com.mauricio.springbootkafkaconsumerexample.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Class to consume kafka messages with manual commit
 */
@Component
public class ExampleConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(ExampleConsumer.class);

    @KafkaListener(topics = "${spring.kafka.consumer.topic-name}", containerFactory = "exampleConcurrentKafkaListenerContainerFactory", autoStartup="${spring.kafka.consumer.enabled}")
    public void consume(String message,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                        @Header(KafkaHeaders.OFFSET) int offset,
                        @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
                        Acknowledgment ack) {
        LOG.info("TEST Kafka details -> Partition: {} Offset: {} Timestamp: {}",
                partition, offset, timestamp);
        LOG.info("TEST Kafka message received: {}", message);
        ack.acknowledge();
    }
}
