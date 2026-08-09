package com.hmdp.kafka;

import com.hmdp.kafka.message.ShopCacheInvalidationMessage;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

@Component
public class ShopCacheInvalidationKafkaProducer {

    private final KafkaTemplate<String, ShopCacheInvalidationMessage> kafkaTemplate;
    private final String topic;

    public ShopCacheInvalidationKafkaProducer(
            @Qualifier("shopCacheKafkaTemplate")
            KafkaTemplate<String, ShopCacheInvalidationMessage> kafkaTemplate,
            @Value("${hmdp.kafka.shop-cache.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public ListenableFuture<SendResult<String, ShopCacheInvalidationMessage>> send(
            ShopCacheInvalidationMessage message) {
        return kafkaTemplate.send(topic, String.valueOf(message.getShopId()), message);
    }
}
