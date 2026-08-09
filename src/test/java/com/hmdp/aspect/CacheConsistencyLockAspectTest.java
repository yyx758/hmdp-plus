package com.hmdp.aspect;

import com.hmdp.annotation.CacheConsistencyLock;
import com.hmdp.enums.CacheLockMode;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CacheConsistencyLockAspectTest {

    @Test
    void acquiresReadLockForQueryAndReleasesItAfterInvocation() throws InterruptedException {
        RedissonClient redissonClient = Mockito.mock(RedissonClient.class);
        RReadWriteLock readWriteLock = Mockito.mock(RReadWriteLock.class);
        RLock readLock = Mockito.mock(RLock.class);
        Mockito.when(redissonClient.getReadWriteLock("lock:cache:consistency:shop:7"))
                .thenReturn(readWriteLock);
        Mockito.when(readWriteLock.readLock()).thenReturn(readLock);
        Mockito.when(readLock.tryLock(3L, TimeUnit.SECONDS)).thenReturn(true);
        Mockito.when(readLock.isHeldByCurrentThread()).thenReturn(true);

        AspectJProxyFactory proxyFactory = new AspectJProxyFactory(new LockedService());
        proxyFactory.addAspect(new CacheConsistencyLockAspect(redissonClient));
        LockedService proxy = proxyFactory.getProxy();

        assertEquals("shop-7", proxy.query(7L));

        InOrder inOrder = Mockito.inOrder(readLock);
        inOrder.verify(readLock).tryLock(3L, TimeUnit.SECONDS);
        inOrder.verify(readLock).isHeldByCurrentThread();
        inOrder.verify(readLock).unlock();
        Mockito.verify(readWriteLock, Mockito.never()).writeLock();
    }

    @Test
    void acquiresWriteLockForUpdateObjectId() throws InterruptedException {
        RedissonClient redissonClient = Mockito.mock(RedissonClient.class);
        RReadWriteLock readWriteLock = Mockito.mock(RReadWriteLock.class);
        RLock writeLock = Mockito.mock(RLock.class);
        Mockito.when(redissonClient.getReadWriteLock("lock:cache:consistency:shop:11"))
                .thenReturn(readWriteLock);
        Mockito.when(readWriteLock.writeLock()).thenReturn(writeLock);
        Mockito.when(writeLock.tryLock(3L, TimeUnit.SECONDS)).thenReturn(true);
        Mockito.when(writeLock.isHeldByCurrentThread()).thenReturn(true);

        AspectJProxyFactory proxyFactory = new AspectJProxyFactory(new LockedService());
        proxyFactory.addAspect(new CacheConsistencyLockAspect(redissonClient));
        LockedService proxy = proxyFactory.getProxy();

        proxy.update(new CacheKey(11L));

        Mockito.verify(writeLock).unlock();
        Mockito.verify(readWriteLock, Mockito.never()).readLock();
    }

    static class LockedService {

        @CacheConsistencyLock(name = "shop", key = "#p0", mode = CacheLockMode.READ)
        public String query(Long shopId) {
            return "shop-" + shopId;
        }

        @CacheConsistencyLock(name = "shop", key = "#p0.id", mode = CacheLockMode.WRITE)
        public void update(CacheKey key) {
        }
    }

    static class CacheKey {
        private final Long id;

        CacheKey(Long id) {
            this.id = id;
        }

        public Long getId() {
            return id;
        }
    }
}
