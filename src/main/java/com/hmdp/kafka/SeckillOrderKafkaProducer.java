package com.hmdp.kafka;

import com.hmdp.kafka.message.SeckillOrderMessage;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

@Component
public class SeckillOrderKafkaProducer {

    private final KafkaTemplate<String, SeckillOrderMessage> kafkaTemplate;
    private final String topic;

    public SeckillOrderKafkaProducer(
            @Qualifier("seckillOrderKafkaTemplate")
            KafkaTemplate<String, SeckillOrderMessage> kafkaTemplate,
            @Value("${hmdp.kafka.seckill-order.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public ListenableFuture<SendResult<String, SeckillOrderMessage>> send(
            SeckillOrderMessage message) {
        return kafkaTemplate.send(topic, String.valueOf(message.getVoucherId()), message);
    }
}
