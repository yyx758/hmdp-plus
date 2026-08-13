package com.hmdp.kafka.outbox;

import com.hmdp.kafka.SeckillVoucherLocalCacheInvalidationKafkaProducer;
import com.hmdp.kafka.message.SeckillVoucherLocalCacheInvalidationMessage;
import com.hmdp.mapper.SeckillVoucherLocalCacheInvalidationOutboxMapper;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOCK_SECKILL_VOUCHER_CACHE_OUTBOX_RELAY;

/** At-least-once relay for cross-instance seckill voucher L1 invalidation. */
@Slf4j
@Component
public class SeckillVoucherLocalCacheInvalidationOutboxRelay {

    private final SeckillVoucherLocalCacheInvalidationOutboxMapper outboxMapper;
    private final SeckillVoucherLocalCacheInvalidationKafkaProducer kafkaProducer;
    private final RedissonClient redissonClient;
    private final int batchSize;
    private final long sendTimeoutSeconds;
    private final int maxAutoRetries;
    private final long initialBackoffSeconds;
    private final long maxBackoffSeconds;

    public SeckillVoucherLocalCacheInvalidationOutboxRelay(
            SeckillVoucherLocalCacheInvalidationOutboxMapper outboxMapper,
            SeckillVoucherLocalCacheInvalidationKafkaProducer kafkaProducer,
            RedissonClient redissonClient,
            @Value("${hmdp.kafka.seckill-voucher-cache.outbox.batch-size:100}") int batchSize,
            @Value("${hmdp.kafka.seckill-voucher-cache.outbox.send-timeout-seconds:10}")
            long sendTimeoutSeconds,
            @Value("${hmdp.kafka.seckill-voucher-cache.outbox.max-auto-retries:12}")
            int maxAutoRetries,
            @Value("${hmdp.kafka.seckill-voucher-cache.outbox.initial-backoff-seconds:1}")
            long initialBackoffSeconds,
            @Value("${hmdp.kafka.seckill-voucher-cache.outbox.max-backoff-seconds:300}")
            long maxBackoffSeconds) {
        this.outboxMapper = outboxMapper;
        this.kafkaProducer = kafkaProducer;
        this.redissonClient = redissonClient;
        this.batchSize = batchSize;
        this.sendTimeoutSeconds = sendTimeoutSeconds;
        this.maxAutoRetries = maxAutoRetries;
        this.initialBackoffSeconds = initialBackoffSeconds;
        this.maxBackoffSeconds = maxBackoffSeconds;
    }

    @Scheduled(fixedDelayString =
            "${hmdp.kafka.seckill-voucher-cache.outbox.poll-delay-ms:200}")
    public void relayPending() {
        RLock relayLock = redissonClient.getLock(LOCK_SECKILL_VOUCHER_CACHE_OUTBOX_RELAY);
        boolean locked = false;
        try {
            locked = relayLock.tryLock();
            if (!locked) {
                return;
            }
            List<SeckillVoucherLocalCacheInvalidationOutboxEvent> events =
                    outboxMapper.findDispatchable(batchSize);
            for (SeckillVoucherLocalCacheInvalidationOutboxEvent event : events) {
                dispatch(event);
            }
        } catch (RuntimeException e) {
            log.error("Seckill voucher L1 invalidation outbox relay failed", e);
        } finally {
            if (locked && relayLock.isHeldByCurrentThread()) {
                relayLock.unlock();
            }
        }
    }

    void dispatch(SeckillVoucherLocalCacheInvalidationOutboxEvent event) {
        SeckillVoucherLocalCacheInvalidationMessage message =
                new SeckillVoucherLocalCacheInvalidationMessage(
                        event.getEventId(),
                        event.getVoucherId(),
                        event.getReason(),
                        event.getCreatedTime().atZone(ZoneId.systemDefault())
                                .toInstant().toEpochMilli());
        try {
            kafkaProducer.send(message).get(sendTimeoutSeconds, TimeUnit.SECONDS);
            if (outboxMapper.markSent(event.getId()) != 1) {
                log.warn("Seckill voucher L1 event sent but not marked SENT, eventId={}",
                        event.getEventId());
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            handleFailure(event, e);
        }
    }

    private void handleFailure(
            SeckillVoucherLocalCacheInvalidationOutboxEvent event, Exception exception) {
        int retryCount = event.getRetryCount() == null ? 0 : event.getRetryCount();
        String error = truncate(exception.getClass().getSimpleName()
                + ": " + exception.getMessage());
        if (retryCount + 1 >= maxAutoRetries) {
            outboxMapper.markFailed(event.getId(), error);
            log.error("Seckill voucher L1 event moved to FAILED, eventId={}, voucherId={}",
                    event.getEventId(), event.getVoucherId(), exception);
            return;
        }
        outboxMapper.scheduleRetry(
                event.getId(),
                LocalDateTime.now().plusSeconds(calculateBackoffSeconds(retryCount)),
                error);
    }

    private long calculateBackoffSeconds(int retryCount) {
        int exponent = Math.min(retryCount, 20);
        long multiplier = 1L << exponent;
        if (initialBackoffSeconds > Long.MAX_VALUE / multiplier) {
            return maxBackoffSeconds;
        }
        return Math.min(initialBackoffSeconds * multiplier, maxBackoffSeconds);
    }

    private String truncate(String value) {
        if (value == null) {
            return null;
        }
        return value.length() <= 512 ? value : value.substring(0, 512);
    }
}
