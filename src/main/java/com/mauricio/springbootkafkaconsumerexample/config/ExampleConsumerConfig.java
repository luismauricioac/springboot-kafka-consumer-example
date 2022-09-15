package com.mauricio.springbootkafkaconsumerexample.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.sql.SQLException;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.mauricio.springbootkafkaconsumerexample.constants.ApplicationConstants.*;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;


@EnableKafka
@Configuration
public class ExampleConsumerConfig {


    @Value(value = "${spring.kafka.consumer.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value = "${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value(value = "${spring.kafka.properties.basic.auth.credentials.source}")
    private String basicAuthCredentialsSource;

    @Value(value = "${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryURL;

    @Value(value = "${spring.kafka.properties.sasl.mechanism}")
    private String saslMechanism;

    @Value(value = "${spring.kafka.properties.sasl.jaas.config}")
    private String saslJaasConfig;

    @Value(value = "${spring.kafka.properties.security.protocol}")
    private String securityProtocol;

    @Value(value = "${spring.kafka.consumer.max-attempts}")
    private int maxAttempts;

    @Value(value = "${spring.kafka.consumer.backoff-period}")
    private int backOffPeriod;

    @Value(value = "${spring.kafka.consumer.enable-auto-commit}")
    private String enableAutoCommit;

    @Value(value = "${spring.kafka.consumer.max-poll-records}")
    private String maxPollRecords;

    @Value(value = "${spring.kafka.consumer.max-poll-interval-ms}")
    private String maxPollIntervalMs;

    @Value(value = "${spring.kafka.consumer.session-timeout-ms}")
    private String sessionTimeoutMs;

    @Value(value = "${spring.kafka.consumer.heartbeat-interval-ms}")
    private String heartbeatIntervalMs;

    @Value(value = "${spring.kafka.consumer.client-id}")
    private String clintIdConfig;

    @Value(value = "${spring.kafka.consumer.request-timeout-ms}")
    private String requestTimeoutMs;


    @Bean("exampleConsumerFactory")
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(BASIC_AUTH_CREDENTIALS_SOURCE, basicAuthCredentialsSource);
        props.put(SCHEMA_REGISTRY_URL, schemaRegistryURL);
        props.put(SASL_MECHANISM, saslMechanism);
        props.put(SECURITY_PROTOCOL, securityProtocol);
        props.put(SASL_JAAS_CONFIG, saslJaasConfig);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs); // has to be less than group.max.session.timeout.ms
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs); // has to be a 1/3 of session.timeout.ms
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clintIdConfig);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs); // has to be a 1/3 of session.timeout.ms
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer()
                , new StringDeserializer());
    }

    @Bean("exampleConcurrentKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> concurrentKafkaListenerFactory
                = new ConcurrentKafkaListenerContainerFactory<>();
        concurrentKafkaListenerFactory.setConsumerFactory(consumerFactory());
        concurrentKafkaListenerFactory.setRetryTemplate(retryTemplate());
        concurrentKafkaListenerFactory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return concurrentKafkaListenerFactory;
    }

    private RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(getSimpleRetryPolicy());
        retryTemplate.setBackOffPolicy(getBackOffPolicy());
        return retryTemplate;
    }

    private SimpleRetryPolicy getSimpleRetryPolicy() {
        Map<Class<? extends Throwable>, Boolean> exceptionMap = new HashMap<>();
        exceptionMap.put(TimeoutException.class, true);
        exceptionMap.put(SQLException.class, true);
        exceptionMap.put(DateTimeParseException.class, true);
        return new SimpleRetryPolicy(maxAttempts, exceptionMap, true);
    }

    private FixedBackOffPolicy getBackOffPolicy() {
        FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
        backOffPolicy.setBackOffPeriod(backOffPeriod);
        return backOffPolicy;
    }
}
