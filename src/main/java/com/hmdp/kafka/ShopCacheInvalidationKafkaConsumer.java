package com.hmdp.kafka;

import com.hmdp.kafka.message.ShopCacheInvalidationMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ShopCacheInvalidationKafkaConsumer {

    private final ShopCacheInvalidationConsumerHandler consumerHandler;

    public ShopCacheInvalidationKafkaConsumer(
            ShopCacheInvalidationConsumerHandler consumerHandler) {
        this.consumerHandler = consumerHandler;
    }

    @KafkaListener(
            topics = "${hmdp.kafka.shop-cache.topic}",
            groupId = "${hmdp.kafka.shop-cache.consumer-group}",
            containerFactory = "shopCacheKafkaListenerContainerFactory"
    )
    public void onMessage(ShopCacheInvalidationMessage message, Acknowledgment acknowledgment) {
        if (message == null || message.getShopId() == null || message.getEventId() == null) {
            throw new IllegalArgumentException("Invalid shop cache invalidation message: " + message);
        }

        consumerHandler.evict(message.getShopId());
        acknowledgment.acknowledge();
        log.debug("Consumed shop cache invalidation, eventId={}, shopId={}",
                message.getEventId(), message.getShopId());
    }
}
