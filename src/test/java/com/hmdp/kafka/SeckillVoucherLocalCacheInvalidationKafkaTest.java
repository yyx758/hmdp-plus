package com.hmdp.kafka;

import com.hmdp.cache.SeckillVoucherLocalCache;
import com.hmdp.kafka.message.SeckillVoucherLocalCacheInvalidationMessage;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;

class SeckillVoucherLocalCacheInvalidationKafkaTest {

    @Test
    void producerUsesVoucherIdAsKafkaKey() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, SeckillVoucherLocalCacheInvalidationMessage> kafkaTemplate =
                Mockito.mock(KafkaTemplate.class);
        SeckillVoucherLocalCacheInvalidationKafkaProducer producer =
                new SeckillVoucherLocalCacheInvalidationKafkaProducer(
                        kafkaTemplate, "voucher-l1-topic");
        SeckillVoucherLocalCacheInvalidationMessage message =
                new SeckillVoucherLocalCacheInvalidationMessage(
                        "event-1", 7L, "voucher-updated", 100L);

        producer.send(message);

        Mockito.verify(kafkaTemplate).send("voucher-l1-topic", "7", message);
    }

    @Test
    void consumerInvalidatesOnlyLocalCacheBeforeAcknowledging() {
        SeckillVoucherLocalCache localCache = Mockito.mock(SeckillVoucherLocalCache.class);
        Acknowledgment acknowledgment = Mockito.mock(Acknowledgment.class);
        SeckillVoucherLocalCacheInvalidationKafkaConsumer consumer =
                new SeckillVoucherLocalCacheInvalidationKafkaConsumer(localCache);
        SeckillVoucherLocalCacheInvalidationMessage message =
                new SeckillVoucherLocalCacheInvalidationMessage(
                        "event-2", 8L, "voucher-updated", 100L);

        consumer.onMessage(message, acknowledgment);

        org.mockito.InOrder inOrder = Mockito.inOrder(localCache, acknowledgment);
        inOrder.verify(localCache).invalidate(8L);
        inOrder.verify(acknowledgment).acknowledge();
    }
}
