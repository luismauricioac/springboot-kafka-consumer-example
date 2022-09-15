package com.mauricio.springbootkafkaconsumerexample.consumer;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.MessageHeaders;

@RunWith(MockitoJUnitRunner.class)
public class ExampleConsumerTest {

    private final String TOPIC_NAME = "localTest";

    private final String MESSAGE = "a message";

    @InjectMocks
    private ExampleConsumer exampleConsumer;

    @Mock
    private Acknowledgment acknowledgment;

    @Mock
    private MessageHeaders headers;

    @Test
    public void consumeSuccessfully()  {

        MockProducer<String, String> mockProducer = new MockProducer<>(false, new StringSerializer(), new StringSerializer());
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, null, MESSAGE);

        mockProducer.send(record);
        exampleConsumer.consume(MESSAGE, 1, 1, 1L, acknowledgment);
    }

}