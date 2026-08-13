package com.hmdp.kafka;

import com.hmdp.kafka.message.SeckillVoucherLocalCacheInvalidationMessage;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

@Component
public class SeckillVoucherLocalCacheInvalidationKafkaProducer {

    private final KafkaTemplate<String, SeckillVoucherLocalCacheInvalidationMessage>
            kafkaTemplate;
    private final String topic;

    public SeckillVoucherLocalCacheInvalidationKafkaProducer(
            @Qualifier("seckillVoucherCacheKafkaTemplate")
            KafkaTemplate<String, SeckillVoucherLocalCacheInvalidationMessage> kafkaTemplate,
            @Value("${hmdp.kafka.seckill-voucher-cache.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public ListenableFuture<SendResult<String, SeckillVoucherLocalCacheInvalidationMessage>>
            send(SeckillVoucherLocalCacheInvalidationMessage message) {
        return kafkaTemplate.send(topic, String.valueOf(message.getVoucherId()), message);
    }
}
