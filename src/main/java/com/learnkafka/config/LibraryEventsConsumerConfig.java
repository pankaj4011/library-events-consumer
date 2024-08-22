package com.learnkafka.config;

import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Value("${topics.retry}")
    private String retryTopic;


    @Value("${topics.retry}")
    private String deadTopic;


    DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(){
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (r, e) -> {
                log.error("Exception in the publishing recoverer {}",e.getMessage(),e);

                    if (e.getCause() instanceof RecoverableDataAccessException) {
                        log.error("Inside the Recoverable exception block");
                        return new TopicPartition(retryTopic, r.partition());
                    }
                    else {
                        return new TopicPartition(deadTopic, r.partition());
                    }
                });
        return recoverer;
    }

    @Bean
    public DefaultErrorHandler errorHandler() {
        var fixedBackOff = new FixedBackOff(1000L, 2);

        var exponentialBackoff = new ExponentialBackOffWithMaxRetries(2);
        exponentialBackoff.setInitialInterval(1_000L);
        exponentialBackoff.setMultiplier(2.0);
        exponentialBackoff.setMaxInterval(2_000L);

        var ignoreExceptionList = List.of(IllegalArgumentException.class);

        var errorHandler = new DefaultErrorHandler(
                deadLetterPublishingRecoverer(),
                //fixedBackOff
                exponentialBackoff);

        ignoreExceptionList.forEach(errorHandler::addNotRetryableExceptions);
        errorHandler
                .setRetryListeners((record, ex, deliveryAttempt) -> {
                    log.info("Failed record in retry listener, exception {}, deliveryAttempt {}",ex.getMessage(),deliveryAttempt);
                });
        return errorHandler;
    }


    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory,kafkaConsumerFactory);
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }

}
