package com.hmdp.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

/**
 * Waits for the broker acknowledgment when recovering a failed consumer record to the DLT.
 *
 * <p>Spring Kafka 2.5 publishes dead-letter records asynchronously by default. If the consumer
 * offset were committed before that asynchronous send failed, the invalidation could be lost.
 * Throwing here keeps the source offset uncommitted so the error handler can try recovery again.</p>
 */
public class SynchronousDeadLetterPublishingRecoverer
        extends DeadLetterPublishingRecoverer {

    private final long sendTimeoutSeconds;

    public SynchronousDeadLetterPublishingRecoverer(
            KafkaOperations<? extends Object, ? extends Object> kafkaOperations,
            BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver,
            long sendTimeoutSeconds) {
        super(kafkaOperations, destinationResolver);
        if (sendTimeoutSeconds <= 0) {
            throw new IllegalArgumentException("DLT send timeout must be positive");
        }
        this.sendTimeoutSeconds = sendTimeoutSeconds;
    }

    @Override
    protected void publish(ProducerRecord<Object, Object> outRecord,
                           KafkaOperations<Object, Object> kafkaOperations) {
        try {
            kafkaOperations.send(outRecord).get(sendTimeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaException("Interrupted while publishing record to DLT", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new KafkaException("Failed to publish record to DLT", e);
        }
    }
}
