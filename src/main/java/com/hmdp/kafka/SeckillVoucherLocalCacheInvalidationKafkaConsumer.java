package com.hmdp.kafka;

import com.hmdp.cache.SeckillVoucherLocalCache;
import com.hmdp.kafka.message.SeckillVoucherLocalCacheInvalidationMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SeckillVoucherLocalCacheInvalidationKafkaConsumer {

    private final SeckillVoucherLocalCache localCache;

    public SeckillVoucherLocalCacheInvalidationKafkaConsumer(
            SeckillVoucherLocalCache localCache) {
        this.localCache = localCache;
    }

    @KafkaListener(
            topics = "${hmdp.kafka.seckill-voucher-cache.topic}",
            groupId = "${hmdp.kafka.seckill-voucher-cache.consumer-group}",
            containerFactory = "seckillVoucherCacheKafkaListenerContainerFactory"
    )
    public void onMessage(
            SeckillVoucherLocalCacheInvalidationMessage message,
            Acknowledgment acknowledgment) {
        if (message == null || message.getVoucherId() == null || message.getEventId() == null) {
            throw new IllegalArgumentException(
                    "Invalid seckill voucher L1 invalidation message: " + message);
        }
        localCache.invalidate(message.getVoucherId());
        acknowledgment.acknowledge();
        log.debug("Consumed seckill voucher L1 invalidation, eventId={}, voucherId={}",
                message.getEventId(), message.getVoucherId());
    }
}
