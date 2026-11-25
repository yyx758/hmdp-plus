package com.hmdp.service;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.kafka.outbox.SeckillOrderOutboxEvent;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_ACCEPTED_AUDIT_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_ACCEPTED_KEY;

/** Advances accepted orders by orderId; it never changes Redis stock or purchase eligibility. */
@Slf4j
@Service
public class SeckillAcceptedOrderAuditService {

    private static final DefaultRedisScript<String> CLAIM_SCRIPT = loadClaimScript();

    private final StringRedisTemplate redisTemplate;
    private final VoucherOrderMapper orderMapper;
    private final SeckillOrderOutboxMapper outboxMapper;
    private final SeckillOrderOutboxBatchWriter batchWriter;
    private final SeckillOrderReceiptService receiptService;
    private final int batchSize;
    private final long retryMillis;

    public SeckillAcceptedOrderAuditService(
            StringRedisTemplate redisTemplate,
            VoucherOrderMapper orderMapper,
            SeckillOrderOutboxMapper outboxMapper,
            SeckillOrderOutboxBatchWriter batchWriter,
            SeckillOrderReceiptService receiptService,
            @Value("${hmdp.seckill.audit.batch-size:200}") int batchSize,
            @Value("${hmdp.seckill.audit.retry-millis:300000}") long retryMillis) {
        this.redisTemplate = redisTemplate;
        this.orderMapper = orderMapper;
        this.outboxMapper = outboxMapper;
        this.batchWriter = batchWriter;
        this.receiptService = receiptService;
        this.batchSize = Math.max(1, batchSize);
        this.retryMillis = Math.max(1000L, retryMillis);
    }

    @Scheduled(fixedDelayString = "${hmdp.seckill.audit.poll-delay-ms:60000}")
    public void auditDueReceipts() {
        for (int index = 0; index < batchSize; index++) {
            String orderId;
            try {
                orderId = redisTemplate.execute(CLAIM_SCRIPT,
                        Collections.singletonList(SECKILL_ORDER_ACCEPTED_AUDIT_KEY),
                        String.valueOf(System.currentTimeMillis()),
                        String.valueOf(retryMillis));
            } catch (RuntimeException e) {
                log.error("Cannot claim accepted-order audit task", e);
                return;
            }
            if (orderId == null) {
                return;
            }
            try {
                auditOne(Long.valueOf(orderId));
            } catch (RuntimeException e) {
                // The Redis audit score was moved forward by a lease. A crash or DB outage
                // causes another attempt after the lease rather than losing the task.
                log.error("Accepted seckill order remains unresolved, orderId={}", orderId, e);
            }
        }
    }

    @Scheduled(fixedDelayString = "${hmdp.seckill.audit.manual-review-delay-ms:300000}")
    public void auditOverdueManualReviews() {
        try {
            List<SeckillOrderOutboxEvent> overdue =
                    outboxMapper.findOverdueManualReview(batchSize);
            if (overdue == null) {
                return;
            }
            for (SeckillOrderOutboxEvent event : overdue) {
                log.error("Seckill order manual review overdue, orderId={}, owner={}, deadline={}",
                        event.getOrderId(), event.getReviewOwner(), event.getReviewDeadline());
            }
        } catch (RuntimeException e) {
            log.error("Cannot audit overdue seckill manual reviews", e);
        }
    }

    void auditOne(Long orderId) {
        Object raw = redisTemplate.opsForHash().get(
                SECKILL_ORDER_ACCEPTED_KEY, orderId.toString());
        if (raw == null) {
            log.error("Accepted-order audit marker has no receipt, orderId={}", orderId);
            return;
        }
        String[] parts = raw.toString().split("\\|", -1);
        if (parts.length != 4 && parts.length != 2) {
            throw new IllegalStateException("Malformed acceptance receipt, orderId=" + orderId);
        }
        Long userId = Long.valueOf(parts[0]);
        Long voucherId = Long.valueOf(parts[1]);
        VoucherOrder order = orderMapper.selectById(orderId);
        if (order != null) {
            verifyIdentity(orderId, userId, voucherId, order.getUserId(), order.getVoucherId());
            SeckillOrderOutboxEvent event = outboxMapper.findByOrderId(orderId);
            if (event != null && !SeckillOrderOutboxEvent.STATUS_COMPLETED.equals(event.getStatus())) {
                outboxMapper.markCompleted(orderId);
            }
            if (Integer.valueOf(4).equals(order.getStatus())) {
                receiptService.markCancelled(order);
            } else {
                receiptService.markSuccess(order);
            }
            return;
        }

        if (parts.length == 4 && ("SUCCESS".equals(parts[3])
                || "CANCELLED".equals(parts[3]))) {
            log.error("Terminal acceptance receipt has no MySQL order; manual data recovery "
                    + "required, orderId={}, receiptStatus={}", orderId, parts[3]);
            return;
        }

        SeckillOrderOutboxEvent event = outboxMapper.findByOrderId(orderId);
        if (event == null) {
            batchWriter.insertCommitted(Collections.singletonList(
                    SeckillOrderOutboxEvent.pending(orderId, voucherId, userId, false)));
            log.warn("Recovered accepted order from Redis receipt, orderId={}", orderId);
            return;
        }
        verifyIdentity(orderId, userId, voucherId, event.getUserId(), event.getVoucherId());
        if (SeckillOrderOutboxEvent.STATUS_MANUAL_REVIEW.equals(event.getStatus())) {
            log.error("Accepted order requires operator review, orderId={}", orderId);
        } else if (SeckillOrderOutboxEvent.STATUS_COMPLETED.equals(event.getStatus())) {
            log.error("Outbox completed but order is absent, orderId={}", orderId);
        } else {
            log.warn("Accepted order still awaiting persistence, orderId={}, status={}",
                    orderId, event.getStatus());
        }
    }

    private void verifyIdentity(Long orderId, Long userId, Long voucherId,
                                Long actualUserId, Long actualVoucherId) {
        if (!userId.equals(actualUserId) || !voucherId.equals(actualVoucherId)) {
            throw new IllegalStateException("Acceptance identity conflicts with persisted data, orderId="
                    + orderId);
        }
    }

    private static DefaultRedisScript<String> loadClaimScript() {
        DefaultRedisScript<String> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(
                new ClassPathResource("lua/seckill_receipt_audit_claim.lua")));
        script.setResultType(String.class);
        return script;
    }
}
