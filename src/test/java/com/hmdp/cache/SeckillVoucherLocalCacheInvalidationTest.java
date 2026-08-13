package com.hmdp.cache;

import com.hmdp.kafka.outbox.SeckillVoucherLocalCacheInvalidationOutboxEvent;
import com.hmdp.mapper.SeckillVoucherLocalCacheInvalidationOutboxMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SeckillVoucherLocalCacheInvalidationTest {

    @Test
    void publisherPersistsOutboxAndInvalidatesImmediatelyWithoutTransaction() {
        SeckillVoucherLocalCacheInvalidationOutboxMapper mapper =
                Mockito.mock(SeckillVoucherLocalCacheInvalidationOutboxMapper.class);
        SeckillVoucherLocalCache localCache = Mockito.mock(SeckillVoucherLocalCache.class);
        Mockito.when(mapper.insert(
                Mockito.any(SeckillVoucherLocalCacheInvalidationOutboxEvent.class)))
                .thenReturn(1);
        SeckillVoucherLocalCacheInvalidationPublisher publisher =
                new SeckillVoucherLocalCacheInvalidationPublisher(mapper, localCache);

        publisher.publish(8L, "voucher-updated");

        ArgumentCaptor<SeckillVoucherLocalCacheInvalidationOutboxEvent> captor =
                ArgumentCaptor.forClass(SeckillVoucherLocalCacheInvalidationOutboxEvent.class);
        Mockito.verify(mapper).insert(captor.capture());
        assertEquals(8L, captor.getValue().getVoucherId());
        assertEquals("voucher-updated", captor.getValue().getReason());
        Mockito.verify(localCache).invalidate(8L);
    }

    @Test
    void publisherDefersLocalInvalidationUntilCommit() {
        SeckillVoucherLocalCacheInvalidationOutboxMapper mapper =
                Mockito.mock(SeckillVoucherLocalCacheInvalidationOutboxMapper.class);
        SeckillVoucherLocalCache localCache = Mockito.mock(SeckillVoucherLocalCache.class);
        Mockito.when(mapper.insert(
                Mockito.any(SeckillVoucherLocalCacheInvalidationOutboxEvent.class)))
                .thenReturn(1);
        SeckillVoucherLocalCacheInvalidationPublisher publisher =
                new SeckillVoucherLocalCacheInvalidationPublisher(mapper, localCache);

        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(true);
        try {
            publisher.publish(9L, null);
            Mockito.verifyNoInteractions(localCache);
            List<TransactionSynchronization> synchronizations =
                    TransactionSynchronizationManager.getSynchronizations();
            assertEquals(1, synchronizations.size());

            synchronizations.get(0).afterCommit();
            Mockito.verify(localCache).invalidate(9L);
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    void failedOutboxInsertFailsBusinessOperation() {
        SeckillVoucherLocalCacheInvalidationOutboxMapper mapper =
                Mockito.mock(SeckillVoucherLocalCacheInvalidationOutboxMapper.class);
        SeckillVoucherLocalCache localCache = Mockito.mock(SeckillVoucherLocalCache.class);
        SeckillVoucherLocalCacheInvalidationPublisher publisher =
                new SeckillVoucherLocalCacheInvalidationPublisher(mapper, localCache);

        assertThrows(IllegalStateException.class,
                () -> publisher.publish(10L, "voucher-updated"));
        Mockito.verifyNoInteractions(localCache);
    }
}
