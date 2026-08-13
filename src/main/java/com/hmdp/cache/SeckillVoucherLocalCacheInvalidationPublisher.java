package com.hmdp.cache;

import com.hmdp.kafka.outbox.SeckillVoucherLocalCacheInvalidationOutboxEvent;
import com.hmdp.mapper.SeckillVoucherLocalCacheInvalidationOutboxMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** Persists a reliable cross-instance L1 invalidation event in the business transaction. */
@Component
public class SeckillVoucherLocalCacheInvalidationPublisher {

    private final SeckillVoucherLocalCacheInvalidationOutboxMapper outboxMapper;
    private final SeckillVoucherLocalCache localCache;

    public SeckillVoucherLocalCacheInvalidationPublisher(
            SeckillVoucherLocalCacheInvalidationOutboxMapper outboxMapper,
            SeckillVoucherLocalCache localCache) {
        this.outboxMapper = outboxMapper;
        this.localCache = localCache;
    }

    public void publish(Long voucherId, String reason) {
        if (voucherId == null) {
            return;
        }
        SeckillVoucherLocalCacheInvalidationOutboxEvent event =
                SeckillVoucherLocalCacheInvalidationOutboxEvent.pending(voucherId, reason);
        if (outboxMapper.insert(event) != 1) {
            throw new IllegalStateException(
                    "Failed to persist seckill voucher L1 invalidation, voucherId=" + voucherId);
        }

        if (TransactionSynchronizationManager.isActualTransactionActive()
                && TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(
                    new TransactionSynchronization() {
                        @Override
                        public void afterCommit() {
                            localCache.invalidate(voucherId);
                        }
                    });
            return;
        }
        localCache.invalidate(voucherId);
    }
}
