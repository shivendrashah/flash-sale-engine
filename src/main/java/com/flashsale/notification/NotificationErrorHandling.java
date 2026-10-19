package com.flashsale.notification;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Retry, then park:
 *   NotificationFailedException  ->  try again max-retries times, retry-backoff apart (3 x 1 s)
 *   still failing                ->  copy the message to flashsale.orders.DLT and move on
 *   UnreadableEventException     ->  no retries: straight to the DLT (reading it again can't help)
 * The message is never lost and never blocks the messages behind it for more than a few seconds.
 */
@Configuration(proxyBeanMethods = false)
class NotificationErrorHandling {

    private static final Logger log = LoggerFactory.getLogger(NotificationErrorHandling.class);

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafka, NotificationProperties properties,
                                          MeterRegistry meters) {
        DeadLetterPublishingRecoverer toDeadLetterTopic = new DeadLetterPublishingRecoverer(kafka,
                (record, exception) -> new TopicPartition(record.topic() + ".DLT", -1));
        ConsumerRecordRecoverer recoverer = (record, exception) -> {
            log.error("Giving up on {}-{}@{}, moving it to the dead-letter topic: {}", record.topic(),
                    record.partition(), record.offset(),
                    NestedExceptionUtils.getMostSpecificCause(exception).getMessage());
            meters.counter("flashsale.notifications", "outcome", "dead_lettered").increment();
            toDeadLetterTopic.accept(record, exception);
        };
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer,
                new FixedBackOff(properties.retryBackoff().toMillis(), properties.maxRetries()));
        handler.addNotRetryableExceptions(UnreadableEventException.class);
        return handler;
    }
}
