package com.hmdp.cache;

import com.hmdp.kafka.outbox.ShopCacheInvalidationOutboxEvent;
import com.hmdp.mapper.ShopCacheInvalidationOutboxMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Adds an invalidation event to the transactional outbox in the same transaction as the shop
 * write. A post-commit fast path evicts this JVM and Redis immediately; the outbox remains the
 * durable source for Kafka delivery to every application instance.
 */
@Component
public class ShopCacheInvalidationPublisher {

    private final ShopCacheInvalidationOutboxMapper outboxMapper;
    private final ShopCacheInvalidationHandler invalidationHandler;

    public ShopCacheInvalidationPublisher(ShopCacheInvalidationOutboxMapper outboxMapper,
                                          ShopCacheInvalidationHandler invalidationHandler) {
        this.outboxMapper = outboxMapper;
        this.invalidationHandler = invalidationHandler;
    }

    public void publish(Long shopId) {
        publish(shopId, "shop-changed");
    }

    public void publish(Long shopId, String reason) {
        if (shopId == null) {
            return;
        }

        ShopCacheInvalidationOutboxEvent outboxEvent =
                ShopCacheInvalidationOutboxEvent.pending(shopId, reason);
        if (outboxMapper.insert(outboxEvent) != 1) {
            throw new IllegalStateException(
                    "Failed to persist shop cache invalidation outbox event, shopId=" + shopId);
        }

        if (TransactionSynchronizationManager.isActualTransactionActive()
                && TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(
                    new TransactionSynchronization() {
                        @Override
                        public void afterCommit() {
                            invalidationHandler.evictBestEffort(shopId);
                        }
                    });
            return;
        }

        invalidationHandler.evictBestEffort(shopId);
    }
}
