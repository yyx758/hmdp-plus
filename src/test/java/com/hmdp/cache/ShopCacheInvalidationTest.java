package com.hmdp.cache;

import com.hmdp.kafka.outbox.ShopCacheInvalidationOutboxEvent;
import com.hmdp.mapper.ShopCacheInvalidationOutboxMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ShopCacheInvalidationTest {

    @Test
    void publisherPersistsOutboxAndEvictsImmediatelyWithoutTransaction() {
        ShopCacheInvalidationOutboxMapper mapper =
                Mockito.mock(ShopCacheInvalidationOutboxMapper.class);
        ShopCacheInvalidationHandler handler = Mockito.mock(ShopCacheInvalidationHandler.class);
        Mockito.when(mapper.insert(Mockito.any(ShopCacheInvalidationOutboxEvent.class)))
                .thenReturn(1);
        ShopCacheInvalidationPublisher publisher =
                new ShopCacheInvalidationPublisher(mapper, handler);

        publisher.publish(8L, "shop-updated");

        ArgumentCaptor<ShopCacheInvalidationOutboxEvent> eventCaptor =
                ArgumentCaptor.forClass(ShopCacheInvalidationOutboxEvent.class);
        Mockito.verify(mapper).insert(eventCaptor.capture());
        assertEquals(8L, eventCaptor.getValue().getShopId());
        assertEquals("shop-updated", eventCaptor.getValue().getReason());
        assertEquals(ShopCacheInvalidationOutboxEvent.STATUS_PENDING,
                eventCaptor.getValue().getStatus());
        Mockito.verify(handler).evictBestEffort(8L);
    }

    @Test
    void publisherDefersFastEvictionUntilTransactionCommit() {
        ShopCacheInvalidationOutboxMapper mapper =
                Mockito.mock(ShopCacheInvalidationOutboxMapper.class);
        ShopCacheInvalidationHandler handler = Mockito.mock(ShopCacheInvalidationHandler.class);
        Mockito.when(mapper.insert(Mockito.any(ShopCacheInvalidationOutboxEvent.class)))
                .thenReturn(1);
        ShopCacheInvalidationPublisher publisher =
                new ShopCacheInvalidationPublisher(mapper, handler);

        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(true);
        try {
            publisher.publish(9L);

            Mockito.verify(handler, Mockito.never()).evictBestEffort(9L);
            List<TransactionSynchronization> synchronizations =
                    TransactionSynchronizationManager.getSynchronizations();
            assertEquals(1, synchronizations.size());

            synchronizations.get(0).afterCommit();
            Mockito.verify(handler).evictBestEffort(9L);
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    void publisherFailsTheBusinessTransactionWhenOutboxInsertFails() {
        ShopCacheInvalidationOutboxMapper mapper =
                Mockito.mock(ShopCacheInvalidationOutboxMapper.class);
        ShopCacheInvalidationHandler handler = Mockito.mock(ShopCacheInvalidationHandler.class);
        Mockito.when(mapper.insert(Mockito.any(ShopCacheInvalidationOutboxEvent.class)))
                .thenReturn(0);
        ShopCacheInvalidationPublisher publisher =
                new ShopCacheInvalidationPublisher(mapper, handler);

        assertThrows(IllegalStateException.class, () -> publisher.publish(10L));
        Mockito.verifyNoInteractions(handler);
    }

    @Test
    void handlerEvictsCaffeineAndRedis() {
        ShopLocalCache localCache = Mockito.mock(ShopLocalCache.class);
        StringRedisTemplate redisTemplate = Mockito.mock(StringRedisTemplate.class);
        ShopCacheInvalidationHandler handler =
                new ShopCacheInvalidationHandler(localCache, redisTemplate);

        handler.evict(11L);

        Mockito.verify(localCache).invalidate(11L);
        Mockito.verify(redisTemplate).delete("cache:shop:11");
    }

    @Test
    void bestEffortHandlerDoesNotBreakPostCommitOnRedisFailure() {
        ShopLocalCache localCache = Mockito.mock(ShopLocalCache.class);
        StringRedisTemplate redisTemplate = Mockito.mock(StringRedisTemplate.class);
        Mockito.when(redisTemplate.delete("cache:shop:12"))
                .thenThrow(new IllegalStateException("redis unavailable"));
        ShopCacheInvalidationHandler handler =
                new ShopCacheInvalidationHandler(localCache, redisTemplate);

        handler.evictBestEffort(12L);

        Mockito.verify(localCache).invalidate(12L);
    }
}
