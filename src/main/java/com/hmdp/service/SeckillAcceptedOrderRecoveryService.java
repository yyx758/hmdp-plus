package com.hmdp.service;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.kafka.outbox.SeckillOrderOutboxEvent;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mapper.SeckillManualReviewActionMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

/** 已受理订单只能持续重试落库，不能回滚用户已经获得的购买资格。 */
@Service
public class SeckillAcceptedOrderRecoveryService {

    private final VoucherOrderMapper voucherOrderMapper;
    private final SeckillOrderOutboxMapper outboxMapper;
    private final SeckillManualReviewActionMapper actionMapper;
    private final long initialBackoffSeconds;
    private final long maxBackoffSeconds;

    public SeckillAcceptedOrderRecoveryService(
            VoucherOrderMapper voucherOrderMapper,
            SeckillOrderOutboxMapper outboxMapper,
            SeckillManualReviewActionMapper actionMapper,
            @Value("${hmdp.kafka.seckill-order.outbox.initial-backoff-seconds:1}")
            long initialBackoffSeconds,
            @Value("${hmdp.kafka.seckill-order.outbox.max-backoff-seconds:300}")
            long maxBackoffSeconds) {
        this.voucherOrderMapper = voucherOrderMapper;
        this.outboxMapper = outboxMapper;
        this.actionMapper = actionMapper;
        this.initialBackoffSeconds = initialBackoffSeconds;
        this.maxBackoffSeconds = maxBackoffSeconds;
    }

    public void retry(SeckillOrderOutboxEvent event, String reason) {
        validate(event);
        VoucherOrder order = voucherOrderMapper.selectById(event.getOrderId());
        if (order != null) {
            verifyIdentity(event, order);
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

    public List<SeckillOrderOutboxEvent> listManualReview(int limit) {
        return outboxMapper.findManualReview(Math.max(1, Math.min(limit, 200)));
    }

    /** Explicit operator action. MANUAL_REVIEW is not an automatic retry queue. */
    @Transactional(rollbackFor = Exception.class)
    public void retryManualReview(Long orderId, Long operatorUserId, String reason) {
        SeckillOrderOutboxEvent event = requireManualReview(orderId, operatorUserId, reason);
        VoucherOrder order = voucherOrderMapper.selectById(orderId);
        if (order != null) {
            verifyIdentity(event, order);
            if (outboxMapper.markCompletedFromManualReview(orderId) != 1) {
                throw new IllegalStateException("人工核查状态已变更，orderId=" + orderId);
            }
        } else {
            int retry = event.getRetryCount() == null ? 0 : event.getRetryCount();
            long backoff = Math.min(maxBackoffSeconds,
                    initialBackoffSeconds * (1L << Math.min(retry, 20)));
            if (outboxMapper.requeueManualReview(orderId,
                    LocalDateTime.now().plusSeconds(backoff),
                    "人工核查后重新进入持久化队列: " + reason.trim()) != 1) {
                throw new IllegalStateException("人工核查状态已变更，orderId=" + orderId);
            }
        }
        if (actionMapper.insert(orderId, operatorUserId, "RETRY", reason.trim()) != 1) {
            throw new IllegalStateException("人工重试审计记录写入失败，orderId=" + orderId);
        }
    }

    @Transactional(rollbackFor = Exception.class)
    public void confirmCompleted(Long orderId, Long operatorUserId, String reason) {
        SeckillOrderOutboxEvent event = requireManualReview(orderId, operatorUserId, reason);
        VoucherOrder order = voucherOrderMapper.selectById(orderId);
        if (order == null) {
            throw new IllegalStateException("订单尚未落库，不能确认完成，orderId=" + orderId);
        }
        verifyIdentity(event, order);
        if (outboxMapper.markCompletedFromManualReview(orderId) != 1
                || actionMapper.insert(orderId, operatorUserId,
                "CONFIRM_COMPLETED", reason.trim()) != 1) {
            throw new IllegalStateException("确认完成失败，orderId=" + orderId);
        }
    }

    /** Records an escalation only. It never decides FAILED or releases a reservation. */
    @Transactional(rollbackFor = Exception.class)
    public void requestFailureReview(Long orderId, Long operatorUserId, String reason) {
        requireManualReview(orderId, operatorUserId, reason);
        if (voucherOrderMapper.selectById(orderId) != null) {
            throw new IllegalStateException("订单已落库，不能发起受理失败审核，orderId=" + orderId);
        }
        if (actionMapper.insert(orderId, operatorUserId,
                "REQUEST_FAILURE_REVIEW", reason.trim()) != 1) {
            throw new IllegalStateException("失败审核申请留痕失败，orderId=" + orderId);
        }
    }

    private SeckillOrderOutboxEvent requireManualReview(
            Long orderId, Long operatorUserId, String reason) {
        if (orderId == null || operatorUserId == null || reason == null
                || reason.trim().isEmpty()) {
            throw new IllegalArgumentException("人工处理必须提供订单、操作人和原因");
        }
        SeckillOrderOutboxEvent event = outboxMapper.findByOrderId(orderId);
        if (event == null || !SeckillOrderOutboxEvent.STATUS_MANUAL_REVIEW.equals(event.getStatus())) {
            throw new IllegalStateException("订单不在人工核查状态，orderId=" + orderId);
        }
        validate(event);
        return event;
    }

    private void verifyIdentity(SeckillOrderOutboxEvent event, VoucherOrder order) {
        if (!event.getUserId().equals(order.getUserId())
                || !event.getVoucherId().equals(order.getVoucherId())) {
            throw new IllegalStateException("已落库订单与受理事件归属冲突，orderId=" + event.getOrderId());
        }
    }

    private void validate(SeckillOrderOutboxEvent event) {
        if (event == null || event.getOrderId() == null || event.getVoucherId() == null
                || event.getUserId() == null) {
            throw new IllegalArgumentException("已受理秒杀事件字段不完整");
        }
    }
}
