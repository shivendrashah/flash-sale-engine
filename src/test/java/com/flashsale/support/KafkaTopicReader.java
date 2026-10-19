package com.flashsale.support;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * Reads every message on a topic with a brand-new consumer group, and returns the ones a test cares about.
 * Kafka keeps messages after they are read, and a new group starts at the oldest one (auto-offset-reset:
 * earliest), so a test can check what is on a topic without disturbing the app's own consumer group.
 */
public final class KafkaTopicReader {

    private KafkaTopicReader() {
    }

    /**
     * Polls {@code topic} until {@code expected} matching messages were seen or {@code timeout} passed, then keeps
     * polling a little longer, so a test can also see that NO extra copy arrived.
     */
    public static List<ConsumerRecord<String, String>> read(ConsumerFactory<?, ?> factory, String topic,
                                                            Predicate<ConsumerRecord<String, String>> wanted,
                                                            int expected, Duration timeout) {
        List<ConsumerRecord<String, String>> found = new ArrayList<>();
        @SuppressWarnings("unchecked")
        ConsumerFactory<String, String> strings = (ConsumerFactory<String, String>) factory;
        try (Consumer<String, String> consumer = strings.createConsumer("test-" + UUID.randomUUID(), "test")) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + timeout.toNanos();
            long quietUntil = Long.MAX_VALUE;
            while (System.nanoTime() < Math.min(deadline, quietUntil)) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                    if (wanted.test(record)) {
                        found.add(record);
                    }
                }
                if (found.size() >= expected && quietUntil == Long.MAX_VALUE) {
                    quietUntil = System.nanoTime() + Duration.ofMillis(500).toNanos();   // look for extra copies
                }
            }
        }
        return found;
    }

    /** A header's value as text, or null. */
    public static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    /** Matches messages whose "eventId" header is one of these. */
    public static Predicate<ConsumerRecord<String, String>> eventIds(List<UUID> eventIds) {
        List<String> ids = eventIds.stream().map(UUID::toString).toList();
        return record -> ids.contains(header(record, "eventId"));
    }
}
