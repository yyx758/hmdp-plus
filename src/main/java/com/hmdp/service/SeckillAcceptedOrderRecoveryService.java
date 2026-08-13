package com.hmdp.service;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.kafka.outbox.SeckillOrderOutboxEvent;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

/** 已受理订单只能持续重试落库，不能回滚用户已经获得的购买资格。 */
@Service
public class SeckillAcceptedOrderRecoveryService {

    private final VoucherOrderMapper voucherOrderMapper;
    private final SeckillOrderOutboxMapper outboxMapper;
    private final long initialBackoffSeconds;
    private final long maxBackoffSeconds;

    public SeckillAcceptedOrderRecoveryService(
            VoucherOrderMapper voucherOrderMapper,
            SeckillOrderOutboxMapper outboxMapper,
            @Value("${hmdp.kafka.seckill-order.outbox.initial-backoff-seconds:1}")
            long initialBackoffSeconds,
            @Value("${hmdp.kafka.seckill-order.outbox.max-backoff-seconds:300}")
            long maxBackoffSeconds) {
        this.voucherOrderMapper = voucherOrderMapper;
        this.outboxMapper = outboxMapper;
        this.initialBackoffSeconds = initialBackoffSeconds;
        this.maxBackoffSeconds = maxBackoffSeconds;
    }

    public void retry(SeckillOrderOutboxEvent event, String reason) {
        validate(event);
        VoucherOrder order = voucherOrderMapper.selectById(event.getOrderId());
        if (order != null) {
            if (outboxMapper.markCompleted(event.getOrderId()) == 0) {
                throw new IllegalStateException("已存在订单无法完成Outbox，orderId=" + event.getOrderId());
            }
            return;
        }
        int retry = event.getRetryCount() == null ? 0 : event.getRetryCount();
        long backoff = Math.min(maxBackoffSeconds,
                initialBackoffSeconds * (1L << Math.min(retry, 20)));
        if (outboxMapper.requeueAccepted(event.getOrderId(),
                LocalDateTime.now().plusSeconds(backoff), reason) == 0) {
            throw new IllegalStateException("已受理订单无法重新进入投递队列，orderId=" + event.getOrderId());
        }
    }

    @Scheduled(fixedDelayString = "${hmdp.kafka.seckill-order.accepted-order-recovery.poll-delay-ms:5000}")
    public void retryManualReviews() {
        List<SeckillOrderOutboxEvent> events = outboxMapper.findManualReview(100);
        for (SeckillOrderOutboxEvent event : events) {
            try {
                retry(event, "人工核对订单重新进入持久化队列");
            } catch (RuntimeException ignored) {
                // MySQL仍不可用，保留MANUAL_REVIEW等待下一轮。
            }
        }
    }

    private void validate(SeckillOrderOutboxEvent event) {
        if (event == null || event.getOrderId() == null || event.getVoucherId() == null
                || event.getUserId() == null) {
            throw new IllegalArgumentException("已受理秒杀事件字段不完整");
        }
    }
}
