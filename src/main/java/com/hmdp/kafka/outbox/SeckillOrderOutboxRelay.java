package com.hmdp.kafka.outbox;

import com.hmdp.kafka.SeckillOrderKafkaProducer;
import com.hmdp.kafka.message.SeckillOrderMessage;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.kafka.support.SendResult;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * 秒杀订单 Outbox 投递任务。
 *
 * <p>使用 MySQL 租约抢占记录，不依赖 Redis 锁。PENDING 发送失败会重试，SENT 长时间没有变成
 * COMPLETED 也会使用同一 eventId/orderId 重新投递，由消费端幂等兜底。</p>
 */
@Slf4j
@Component
public class SeckillOrderOutboxRelay {

    private static final int DEADLOCK_RETRIES = 3;

    private final SeckillOrderOutboxMapper outboxMapper;
    private final SeckillOrderKafkaProducer producer;
    private final String relayOwner = UUID.randomUUID().toString();
    private final int batchSize;
    private final long sendTimeoutSeconds;
    private final long relayLeaseSeconds;
    private final long sentRecheckSeconds;
    private final long initialBackoffSeconds;
    private final long maxBackoffSeconds;

    public SeckillOrderOutboxRelay(
            SeckillOrderOutboxMapper outboxMapper,
            SeckillOrderKafkaProducer producer,
            @Value("${hmdp.kafka.seckill-order.outbox.batch-size:100}") int batchSize,
            @Value("${hmdp.kafka.seckill-order.outbox.send-timeout-seconds:10}")
            long sendTimeoutSeconds,
            @Value("${hmdp.kafka.seckill-order.outbox.relay-lease-seconds:30}")
            long relayLeaseSeconds,
            @Value("${hmdp.kafka.seckill-order.outbox.sent-recheck-seconds:60}")
            long sentRecheckSeconds,
            @Value("${hmdp.kafka.seckill-order.outbox.initial-backoff-seconds:1}")
            long initialBackoffSeconds,
            @Value("${hmdp.kafka.seckill-order.outbox.max-backoff-seconds:300}")
            long maxBackoffSeconds) {
        this.outboxMapper = outboxMapper;
        this.producer = producer;
        this.batchSize = batchSize;
        this.sendTimeoutSeconds = sendTimeoutSeconds;
        this.relayLeaseSeconds = relayLeaseSeconds;
        this.sentRecheckSeconds = sentRecheckSeconds;
        this.initialBackoffSeconds = initialBackoffSeconds;
        this.maxBackoffSeconds = maxBackoffSeconds;
    }

    @Scheduled(fixedDelayString = "${hmdp.kafka.seckill-order.outbox.poll-delay-ms:100}")
    public void relayPending() {
        try {
            List<SeckillOrderOutboxEvent> events = outboxMapper.findDispatchable(batchSize);
            if (events == null || events.isEmpty()) {
                return;
            }
            List<Long> candidateIds = eventIds(events);
            LocalDateTime leaseUntil = LocalDateTime.now().plusSeconds(relayLeaseSeconds);
            if (outboxMapper.claimRelayBatch(candidateIds, relayOwner, leaseUntil) == 0) {
                return;
            }
            List<SeckillOrderOutboxEvent> claimed =
                    outboxMapper.findClaimedBatch(candidateIds, relayOwner);
            if (claimed != null && !claimed.isEmpty()) {
                dispatchBatch(claimed);
            }
        } catch (RuntimeException e) {
            log.error("秒杀订单 Outbox 投递任务异常", e);
        }
    }

    void dispatch(SeckillOrderOutboxEvent event) {
        dispatchBatch(Collections.singletonList(event));
    }

    void dispatchBatch(List<SeckillOrderOutboxEvent> events) {
        List<PendingSend> pending = new ArrayList<>(events.size());
        for (SeckillOrderOutboxEvent event : events) {
            try {
                pending.add(new PendingSend(event, producer.send(toMessage(event))));
            } catch (RuntimeException e) {
                pending.add(new PendingSend(event, e));
            }
        }

        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(sendTimeoutSeconds);
        List<Long> sentIds = new ArrayList<>();
        Map<Long, List<Long>> failedIdsByBackoff = new LinkedHashMap<>();
        String lastError = null;
        for (PendingSend send : pending) {
            Exception failure = send.immediateFailure;
            if (failure == null) {
                try {
                    long remaining = deadlineNanos - System.nanoTime();
                    if (remaining <= 0) {
                        throw new TimeoutException("Kafka batch send timed out");
                    }
                    send.future.get(remaining, TimeUnit.NANOSECONDS);
                    sentIds.add(send.event.getId());
                    continue;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    failure = e;
                } catch (ExecutionException | TimeoutException e) {
                    failure = e;
                }
            }
            long backoff = backoffSeconds(send.event);
            failedIdsByBackoff.computeIfAbsent(backoff, ignored -> new ArrayList<>())
                    .add(send.event.getId());
            lastError = truncate(failure.getClass().getSimpleName() + ": " + failure.getMessage());
        }

        if (!sentIds.isEmpty()) {
            Collections.sort(sentIds);
            markSentWithDeadlockRetry(sentIds,
                    LocalDateTime.now().plusSeconds(sentRecheckSeconds));
        }
        for (Map.Entry<Long, List<Long>> failed : failedIdsByBackoff.entrySet()) {
            outboxMapper.scheduleRetryBatch(failed.getValue(), relayOwner,
                    LocalDateTime.now().plusSeconds(failed.getKey()), lastError);
        }
    }

    private void markSentWithDeadlockRetry(List<Long> ids, LocalDateTime nextCheckTime) {
        for (int attempt = 1; ; attempt++) {
            try {
                outboxMapper.markSentBatch(ids, relayOwner, nextCheckTime);
                return;
            } catch (DeadlockLoserDataAccessException e) {
                if (attempt >= DEADLOCK_RETRIES) {
                    throw e;
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(10L * attempt);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }

    private SeckillOrderMessage toMessage(SeckillOrderOutboxEvent event) {
        return new SeckillOrderMessage(
                event.getEventId(), event.getId(), event.getOrderId(),
                event.getVoucherId(), event.getUserId(),
                event.getAutoIssued(), event.getCreatedTime().atZone(ZoneId.systemDefault())
                        .toInstant().toEpochMilli());
    }

    private List<Long> eventIds(List<SeckillOrderOutboxEvent> events) {
        return events.stream().map(SeckillOrderOutboxEvent::getId).collect(Collectors.toList());
    }

    private long backoffSeconds(SeckillOrderOutboxEvent event) {
        int retry = event.getRetryCount() == null ? 0 : event.getRetryCount();
        return Math.min(maxBackoffSeconds,
                initialBackoffSeconds * (1L << Math.min(retry, 20)));
    }

    private String truncate(String value) {
        return value == null || value.length() <= 500 ? value : value.substring(0, 500);
    }

    private static final class PendingSend {
        private final SeckillOrderOutboxEvent event;
        private final ListenableFuture<SendResult<String, SeckillOrderMessage>> future;
        private final Exception immediateFailure;

        private PendingSend(
                SeckillOrderOutboxEvent event,
                ListenableFuture<SendResult<String, SeckillOrderMessage>> future) {
            this.event = event;
            this.future = future;
            this.immediateFailure = null;
        }

        private PendingSend(SeckillOrderOutboxEvent event, Exception immediateFailure) {
            this.event = event;
            this.future = null;
            this.immediateFailure = immediateFailure;
        }
    }
}
