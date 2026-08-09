package com.hmdp.kafka;

import com.hmdp.cache.ShopCacheInvalidationHandler;
import com.hmdp.kafka.message.ShopCacheInvalidationMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaOperations;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.SettableListenableFuture;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ShopCacheInvalidationKafkaTest {

    @Test
    void producerUsesShopIdAsKafkaKey() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, ShopCacheInvalidationMessage> kafkaTemplate =
                Mockito.mock(KafkaTemplate.class);
        ShopCacheInvalidationKafkaProducer producer =
                new ShopCacheInvalidationKafkaProducer(kafkaTemplate, "shop-cache-topic");
        ShopCacheInvalidationMessage message =
                new ShopCacheInvalidationMessage("event-1", 7L, "shop-updated", 100L);

        producer.send(message);

        Mockito.verify(kafkaTemplate).send("shop-cache-topic", "7", message);
    }

    @Test
    void consumerAcknowledgesOnlyAfterSuccessfulEviction() {
        ShopCacheInvalidationConsumerHandler handler =
                Mockito.mock(ShopCacheInvalidationConsumerHandler.class);
        Acknowledgment acknowledgment = Mockito.mock(Acknowledgment.class);
        ShopCacheInvalidationKafkaConsumer consumer =
                new ShopCacheInvalidationKafkaConsumer(handler);
        ShopCacheInvalidationMessage message =
                new ShopCacheInvalidationMessage("event-2", 8L, "shop-updated", 100L);

        consumer.onMessage(message, acknowledgment);

        Mockito.verify(handler).evict(8L);
        Mockito.verify(acknowledgment).acknowledge();
    }

    @Test
    void consumerDoesNotAcknowledgeFailedEviction() {
        ShopCacheInvalidationConsumerHandler handler =
                Mockito.mock(ShopCacheInvalidationConsumerHandler.class);
        Acknowledgment acknowledgment = Mockito.mock(Acknowledgment.class);
        Mockito.doThrow(new IllegalStateException("redis unavailable"))
                .when(handler).evict(9L);
        ShopCacheInvalidationKafkaConsumer consumer =
                new ShopCacheInvalidationKafkaConsumer(handler);
        ShopCacheInvalidationMessage message =
                new ShopCacheInvalidationMessage("event-3", 9L, "shop-updated", 100L);

        assertThrows(IllegalStateException.class,
                () -> consumer.onMessage(message, acknowledgment));

        Mockito.verify(acknowledgment, Mockito.never()).acknowledge();
    }

    @Test
    void distributedHandlerDelegatesToStrictEviction() {
        ShopCacheInvalidationHandler invalidationHandler =
                Mockito.mock(ShopCacheInvalidationHandler.class);
        ShopCacheInvalidationConsumerHandler handler =
                new ShopCacheInvalidationConsumerHandler(invalidationHandler);

        handler.evict(10L);

        Mockito.verify(invalidationHandler).evict(10L);
    }

    @Test
    void dltRecoveryFailsWhenBrokerDoesNotAcknowledgeTheDltRecord() {
        @SuppressWarnings("unchecked")
        KafkaOperations<Object, Object> kafkaOperations = Mockito.mock(KafkaOperations.class);
        SettableListenableFuture<SendResult<Object, Object>> future =
                new SettableListenableFuture<>();
        future.setException(new IllegalStateException("dlt broker unavailable"));
        Mockito.when(kafkaOperations.send(Mockito.any(ProducerRecord.class)))
                .thenReturn(future);
        SynchronousDeadLetterPublishingRecoverer recoverer =
                new SynchronousDeadLetterPublishingRecoverer(
                        kafkaOperations,
                        (record, exception) -> new TopicPartition("shop-cache.DLT", 0),
                        1
                );
        ConsumerRecord<String, ShopCacheInvalidationMessage> failedRecord =
                new ConsumerRecord<>(
                        "shop-cache",
                        0,
                        5L,
                        "7",
                        new ShopCacheInvalidationMessage("event-4", 7L, "shop-updated", 100L)
                );

        assertThrows(KafkaException.class,
                () -> recoverer.accept(failedRecord, new IllegalStateException("redis unavailable")));
    }
}
