package com.hmdp.kafka.outbox;

import com.hmdp.kafka.ShopCacheInvalidationKafkaProducer;
import com.hmdp.kafka.message.ShopCacheInvalidationMessage;
import com.hmdp.mapper.ShopCacheInvalidationOutboxMapper;
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

import static com.hmdp.utils.RedisConstants.LOCK_SHOP_CACHE_OUTBOX_RELAY;

/**
 * At-least-once outbox relay. A crash after Kafka accepts a record but before markSent may produce
 * a duplicate, which is safe because cache eviction is idempotent.
 */
@Slf4j
@Component
public class ShopCacheInvalidationOutboxRelay {

    private final ShopCacheInvalidationOutboxMapper outboxMapper;
    private final ShopCacheInvalidationKafkaProducer kafkaProducer;
    private final RedissonClient redissonClient;
    private final int batchSize;
    private final long sendTimeoutSeconds;
    private final int maxAutoRetries;
    private final long initialBackoffSeconds;
    private final long maxBackoffSeconds;

    public ShopCacheInvalidationOutboxRelay(
            ShopCacheInvalidationOutboxMapper outboxMapper,
            ShopCacheInvalidationKafkaProducer kafkaProducer,
            RedissonClient redissonClient,
            @Value("${hmdp.kafka.shop-cache.outbox.batch-size:100}") int batchSize,
            @Value("${hmdp.kafka.shop-cache.outbox.send-timeout-seconds:10}")
            long sendTimeoutSeconds,
            @Value("${hmdp.kafka.shop-cache.outbox.max-auto-retries:12}") int maxAutoRetries,
            @Value("${hmdp.kafka.shop-cache.outbox.initial-backoff-seconds:1}")
            long initialBackoffSeconds,
            @Value("${hmdp.kafka.shop-cache.outbox.max-backoff-seconds:300}")
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

    @Scheduled(fixedDelayString = "${hmdp.kafka.shop-cache.outbox.poll-delay-ms:200}")
    public void relayPending() {
        RLock relayLock = redissonClient.getLock(LOCK_SHOP_CACHE_OUTBOX_RELAY);
        boolean locked = false;
        try {
            locked = relayLock.tryLock();
            if (!locked) {
                return;
            }
            List<ShopCacheInvalidationOutboxEvent> events =
                    outboxMapper.findDispatchable(batchSize);
            for (ShopCacheInvalidationOutboxEvent event : events) {
                dispatch(event);
            }
        } catch (RuntimeException e) {
            log.error("Shop cache invalidation outbox relay failed", e);
        } finally {
            if (locked && relayLock.isHeldByCurrentThread()) {
                relayLock.unlock();
            }
        }
    }

    void dispatch(ShopCacheInvalidationOutboxEvent event) {
        ShopCacheInvalidationMessage message = new ShopCacheInvalidationMessage(
                event.getEventId(),
                event.getShopId(),
                event.getReason(),
                event.getCreatedTime()
                        .atZone(ZoneId.systemDefault())
                        .toInstant()
                        .toEpochMilli()
        );

        try {
            kafkaProducer.send(message).get(sendTimeoutSeconds, TimeUnit.SECONDS);
            if (outboxMapper.markSent(event.getId()) != 1) {
                log.warn("Outbox event was sent but not marked SENT, eventId={}",
                        event.getEventId());
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            handleFailure(event, e);
        }
    }

    private void handleFailure(ShopCacheInvalidationOutboxEvent event, Exception exception) {
        int currentRetryCount = event.getRetryCount() == null ? 0 : event.getRetryCount();
        String error = truncate(exception.getClass().getSimpleName() + ": " + exception.getMessage());
        if (currentRetryCount + 1 >= maxAutoRetries) {
            outboxMapper.markFailed(event.getId(), error);
            log.error("Outbox event moved to FAILED parking lot, eventId={}, shopId={}",
                    event.getEventId(), event.getShopId(), exception);
            return;
        }

        long backoffSeconds = calculateBackoffSeconds(currentRetryCount);
        outboxMapper.scheduleRetry(
                event.getId(),
                LocalDateTime.now().plusSeconds(backoffSeconds),
                error
        );
        log.warn("Kafka send failed; outbox event scheduled for retry, eventId={}, retry={}, "
                        + "backoffSeconds={}",
                event.getEventId(), currentRetryCount + 1, backoffSeconds);
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
