package com.hmdp.service;

import com.hmdp.cache.SeckillVoucherBloomFilter;
import com.hmdp.cache.SeckillVoucherLocalCache;
import com.hmdp.dto.SeckillVoucherCacheDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class SeckillVoucherCacheServiceTest {

    private SeckillVoucherLocalCache localCache;
    private SeckillVoucherBloomFilter bloomFilter;
    private StringRedisTemplate redisTemplate;
    private HashOperations<String, Object, Object> hashOperations;
    private ValueOperations<String, String> valueOperations;
    private RedissonClient redissonClient;
    private RLock lock;
    private ISeckillVoucherService seckillVoucherService;
    private IVoucherService voucherService;
    private SeckillVoucherRedisSynchronizer redisSynchronizer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        localCache = Mockito.mock(SeckillVoucherLocalCache.class);
        bloomFilter = Mockito.mock(SeckillVoucherBloomFilter.class);
        redisTemplate = Mockito.mock(StringRedisTemplate.class);
        hashOperations = Mockito.mock(HashOperations.class);
        valueOperations = Mockito.mock(ValueOperations.class);
        redissonClient = Mockito.mock(RedissonClient.class);
        lock = Mockito.mock(RLock.class);
        seckillVoucherService = Mockito.mock(ISeckillVoucherService.class);
        voucherService = Mockito.mock(IVoucherService.class);
        redisSynchronizer = Mockito.mock(SeckillVoucherRedisSynchronizer.class);
        Mockito.when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        Mockito.when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        Mockito.when(redissonClient.getLock("lock:seckill:meta:rebuild:2")).thenReturn(lock);
        Mockito.when(bloomFilter.mightContain(2L)).thenReturn(true);
    }

    @Test
    void localHitDoesNotAccessBloomRedisOrDatabase() {
        SeckillVoucherCacheDTO localValue = validCacheValue();
        Mockito.when(localCache.get(2L)).thenReturn(localValue);

        SeckillVoucherCacheDTO result = service(Runnable::run).queryById(2L);

        assertSame(localValue, result);
        Mockito.verifyNoInteractions(bloomFilter, redisTemplate, seckillVoucherService, voucherService);
    }

    @Test
    void bloomFilterRejectsIllegalIdBeforeRedis() {
        Mockito.when(bloomFilter.mightContain(2L)).thenReturn(false);

        SeckillVoucherCacheDTO result = service(Runnable::run).queryById(2L);

        assertNull(result);
        Mockito.verifyNoInteractions(redisTemplate, seckillVoucherService, voucherService);
    }

    @Test
    void redisHitWarmsLocalCache() {
        Mockito.when(hashOperations.entries("seckill:meta:2"))
                .thenReturn(redisMetadata(System.currentTimeMillis() + 60_000));

        SeckillVoucherCacheDTO result = service(Runnable::run).queryById(2L);

        assertEquals(2L, result.getVoucherId());
        assertEquals(10L, result.getShopId());
        Mockito.verify(localCache).put(result);
        Mockito.verifyNoInteractions(seckillVoucherService, voucherService);
    }

    @Test
    void cachedNullShortCircuitsDatabaseLookup() {
        Mockito.when(hashOperations.entries("seckill:meta:2"))
                .thenReturn(Collections.emptyMap());
        Mockito.when(redisTemplate.hasKey("seckill:meta:null:2")).thenReturn(true);

        assertNull(service(Runnable::run).queryById(2L));

        Mockito.verifyNoInteractions(redissonClient, seckillVoucherService, voucherService);
    }

    @Test
    void distributedLockPerformsDoubleCheckBeforeDatabase() throws InterruptedException {
        Mockito.when(hashOperations.entries("seckill:meta:2"))
                .thenReturn(Collections.emptyMap())
                .thenReturn(redisMetadata(System.currentTimeMillis() + 60_000));
        Mockito.when(redisTemplate.hasKey("seckill:meta:null:2")).thenReturn(false);
        Mockito.when(lock.tryLock(200L, 10_000L, TimeUnit.MILLISECONDS)).thenReturn(true);
        Mockito.when(lock.isHeldByCurrentThread()).thenReturn(true);

        SeckillVoucherCacheDTO result = service(Runnable::run).queryById(2L);

        assertEquals(2L, result.getVoucherId());
        Mockito.verifyNoInteractions(seckillVoucherService, voucherService);
        Mockito.verify(lock).unlock();
    }

    @Test
    void databaseMissWritesShortLivedNullValue() throws InterruptedException {
        Mockito.when(hashOperations.entries("seckill:meta:2"))
                .thenReturn(Collections.emptyMap());
        Mockito.when(redisTemplate.hasKey("seckill:meta:null:2")).thenReturn(false);
        Mockito.when(lock.tryLock(200L, 10_000L, TimeUnit.MILLISECONDS)).thenReturn(true);
        Mockito.when(lock.isHeldByCurrentThread()).thenReturn(true);

        assertNull(service(Runnable::run).queryById(2L));

        Mockito.verify(valueOperations).set(
                Mockito.eq("seckill:meta:null:2"),
                Mockito.eq("1"),
                Mockito.longThat(ttl -> ttl >= 120L && ttl <= 150L),
                Mockito.eq(TimeUnit.SECONDS));
        Mockito.verify(localCache).invalidate(2L);
    }

    @Test
    void lockContentionDoesNotFallThroughToDatabase() throws InterruptedException {
        Mockito.when(hashOperations.entries("seckill:meta:2"))
                .thenReturn(Collections.emptyMap());
        Mockito.when(redisTemplate.hasKey("seckill:meta:null:2")).thenReturn(false);
        Mockito.when(lock.tryLock(200L, 10_000L, TimeUnit.MILLISECONDS)).thenReturn(false);

        assertNull(service(Runnable::run).queryById(2L));

        Mockito.verifyNoInteractions(seckillVoucherService, voucherService);
    }

    @Test
    void logicalExpiryReturnsStaleValueAndRebuildsAsynchronously() throws InterruptedException {
        AtomicReference<Runnable> submittedTask = new AtomicReference<>();
        Executor capturingExecutor = submittedTask::set;
        Mockito.when(hashOperations.entries("seckill:meta:2"))
                .thenReturn(redisMetadata(System.currentTimeMillis() - 1));
        Mockito.when(lock.tryLock(0L, 10_000L, TimeUnit.MILLISECONDS)).thenReturn(true);
        Mockito.when(lock.isHeldByCurrentThread()).thenReturn(true);
        LocalDateTime begin = LocalDateTime.now().minusHours(1);
        LocalDateTime end = LocalDateTime.now().plusHours(1);
        SeckillVoucher databaseSeckillVoucher = new SeckillVoucher()
                .setVoucherId(2L).setBeginTime(begin).setEndTime(end);
        Voucher databaseVoucher = new Voucher()
                .setId(2L).setShopId(10L).setStatus(1);
        Mockito.when(seckillVoucherService.getById(2L)).thenReturn(databaseSeckillVoucher);
        Mockito.when(voucherService.getById(2L)).thenReturn(databaseVoucher);

        SeckillVoucherCacheDTO stale = service(capturingExecutor).queryById(2L);

        assertEquals(2L, stale.getVoucherId());
        Mockito.verifyNoInteractions(seckillVoucherService, voucherService);
        submittedTask.get().run();
        Mockito.verify(redisSynchronizer)
                .synchronizeMetadata(databaseSeckillVoucher, databaseVoucher);
        Mockito.verify(bloomFilter).put(2L);
        Mockito.verify(lock).unlock();
    }

    private SeckillVoucherCacheService service(Executor executor) {
        return new SeckillVoucherCacheService(
                localCache,
                bloomFilter,
                redisTemplate,
                redissonClient,
                seckillVoucherService,
                voucherService,
                redisSynchronizer,
                executor,
                120L,
                200L,
                10_000L);
    }

    private SeckillVoucherCacheDTO validCacheValue() {
        return new SeckillVoucherCacheDTO()
                .setVoucherId(2L)
                .setShopId(10L)
                .setStatus(1)
                .setBeginTime(LocalDateTime.now().minusHours(1))
                .setEndTime(LocalDateTime.now().plusHours(1));
    }

    private Map<Object, Object> redisMetadata(long logicalExpireAt) {
        Map<Object, Object> metadata = new HashMap<>();
        metadata.put("voucherId", "2");
        metadata.put("shopId", "10");
        metadata.put("status", "1");
        metadata.put("beginTime", String.valueOf(toEpochMillis(LocalDateTime.now().minusHours(1))));
        metadata.put("endTime", String.valueOf(toEpochMillis(LocalDateTime.now().plusHours(1))));
        metadata.put("logicalExpireAt", String.valueOf(logicalExpireAt));
        return metadata;
    }

    private long toEpochMillis(LocalDateTime time) {
        return time.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
}
