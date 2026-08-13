package com.hmdp.kafka;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.kafka.message.SeckillOrderMessage;
import com.hmdp.kafka.outbox.SeckillOrderOutboxEvent;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import com.hmdp.service.ISeckillTopBuyerService;
import com.hmdp.service.SeckillAcceptedOrderRecoveryService;
import com.hmdp.service.SeckillOrderConsumerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class SeckillOrderKafkaConsumer {

    private final SeckillOrderConsumerService consumerService;
    private final SeckillOrderOutboxMapper outboxMapper;
    private final ISeckillTopBuyerService topBuyerService;
    private final SeckillAcceptedOrderRecoveryService acceptedOrderRecoveryService;

    public SeckillOrderKafkaConsumer(
            SeckillOrderConsumerService consumerService,
            SeckillOrderOutboxMapper outboxMapper,
            ISeckillTopBuyerService topBuyerService,
            SeckillAcceptedOrderRecoveryService acceptedOrderRecoveryService) {
        this.consumerService = consumerService;
        this.outboxMapper = outboxMapper;
        this.topBuyerService = topBuyerService;
        this.acceptedOrderRecoveryService = acceptedOrderRecoveryService;
    }

    @KafkaListener(
            topics = "${hmdp.kafka.seckill-order.topic}",
            groupId = "${hmdp.kafka.seckill-order.consumer-group}",
            containerFactory = "seckillOrderKafkaListenerContainerFactory")
    public void onMessage(List<SeckillOrderMessage> messages, Acknowledgment acknowledgment) {
        List<VoucherOrder> orders = consumerService.createOrders(messages);
        acknowledgment.acknowledge();
        for (VoucherOrder order : orders) {
            afterOrderCreated(order);
        }
    }

    @KafkaListener(
            topics = "${hmdp.kafka.seckill-order.dlt-topic}",
            groupId = "${hmdp.kafka.seckill-order.dlt-consumer-group}",
            containerFactory = "seckillOrderKafkaListenerContainerFactory")
    public void onDeadLetter(List<SeckillOrderMessage> messages, Acknowledgment acknowledgment) {
        for (SeckillOrderMessage message : messages) {
            recoverDeadLetter(message);
        }
        acknowledgment.acknowledge();
    }

    private void recoverDeadLetter(SeckillOrderMessage message) {
        if (message == null || message.getOrderId() == null) {
            throw new IllegalArgumentException("Incomplete seckill dead-letter message");
        }
        SeckillOrderOutboxEvent event = outboxMapper.findByOrderId(message.getOrderId());
        if (event == null) {
            throw new IllegalStateException(
                    "Outbox event for dead letter is missing, orderId=" + message.getOrderId());
        }
        try {
            acceptedOrderRecoveryService.retry(event,
                    "Kafka consumption retries exhausted; accepted order is queued again");
        } catch (RuntimeException e) {
            outboxMapper.markManualReview(message.getOrderId(),
                    "Failed to requeue accepted order: " + e.getMessage());
            log.error("Failed to requeue accepted seckill order; manual review required, orderId={}",
                    message.getOrderId(), e);
        }
    }

    private void afterOrderCreated(VoucherOrder order) {
        try {
            topBuyerService.recordSuccessfulOrder(order);
        } catch (RuntimeException e) {
            log.error("Post-order marketing processing failed, orderId={}", order.getId(), e);
        }
    }
}
