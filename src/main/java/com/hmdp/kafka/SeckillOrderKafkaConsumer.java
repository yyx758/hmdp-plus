package com.hmdp.kafka;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.kafka.message.SeckillOrderMessage;
import com.hmdp.kafka.outbox.SeckillOrderOutboxEvent;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import com.hmdp.service.ISeckillTopBuyerService;
import com.hmdp.service.SeckillOrderConsumerService;
import com.hmdp.service.SeckillOrderReceiptService;
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
    private final SeckillOrderReceiptService receiptService;

    public SeckillOrderKafkaConsumer(
            SeckillOrderConsumerService consumerService,
            SeckillOrderOutboxMapper outboxMapper,
            ISeckillTopBuyerService topBuyerService,
            SeckillOrderReceiptService receiptService) {
        this.consumerService = consumerService;
        this.outboxMapper = outboxMapper;
        this.topBuyerService = topBuyerService;
        this.receiptService = receiptService;
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
        if (!event.getUserId().equals(message.getUserId())
                || !event.getVoucherId().equals(message.getVoucherId())) {
            throw new IllegalStateException(
                    "Dead-letter identity conflicts with Outbox, orderId=" + message.getOrderId());
        }
        if (SeckillOrderOutboxEvent.STATUS_COMPLETED.equals(event.getStatus())) {
            return;
        }
        int parked = outboxMapper.markManualReview(message.getOrderId(),
                "Kafka consumption retries exhausted; accepted order requires review");
        if (parked != 1) {
            throw new IllegalStateException(
                    "Could not park accepted order for review, orderId=" + message.getOrderId());
        }
        log.error("Accepted seckill order parked for manual review, orderId={}",
                message.getOrderId());
    }

    private void afterOrderCreated(VoucherOrder order) {
        try {
            receiptService.markSuccess(order);
        } catch (RuntimeException e) {
            log.error("Could not refresh accepted-order receipt after commit, orderId={}",
                    order.getId(), e);
        }
        try {
            topBuyerService.recordSuccessfulOrder(order);
        } catch (RuntimeException e) {
            log.error("Post-order marketing processing failed, orderId={}", order.getId(), e);
        }
    }
}
