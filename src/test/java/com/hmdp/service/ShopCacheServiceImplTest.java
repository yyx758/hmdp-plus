package com.hmdp.service;

import cn.hutool.json.JSONUtil;
import com.hmdp.cache.ShopBloomFilter;
import com.hmdp.cache.ShopLocalCache;
import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopCacheServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class ShopCacheServiceImplTest {

    private ShopLocalCache localCache;
    private ShopBloomFilter bloomFilter;
    private StringRedisTemplate redisTemplate;
    private ValueOperations<String, String> valueOperations;
    private RedissonClient redissonClient;
    private RLock lock;
    private Function<Long, Shop> databaseFallback;
    private IShopCacheService cacheService;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        localCache = Mockito.mock(ShopLocalCache.class);
        bloomFilter = Mockito.mock(ShopBloomFilter.class);
        redisTemplate = Mockito.mock(StringRedisTemplate.class);
        valueOperations = Mockito.mock(ValueOperations.class);
        redissonClient = Mockito.mock(RedissonClient.class);
        lock = Mockito.mock(RLock.class);
        databaseFallback = Mockito.mock(Function.class);
        Mockito.when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        Mockito.when(bloomFilter.mightContain(2L)).thenReturn(true);
        Mockito.when(redissonClient.getLock("lock:shop:2")).thenReturn(lock);
        cacheService = new ShopCacheServiceImpl(
                localCache, bloomFilter, redisTemplate, redissonClient,
                30L, 120L, 500L, 10_000L);
    }

    @Test
    void localHitSkipsBloomRedisAndDatabase() {
        Shop localValue = shop();
        Mockito.when(localCache.get(2L)).thenReturn(localValue);

        assertSame(localValue, cacheService.queryById(2L, databaseFallback));

        Mockito.verifyNoInteractions(bloomFilter, redisTemplate, databaseFallback);
    }

    @Test
    void bloomFilterRejectsIllegalIdBeforeRedis() {
        Mockito.when(bloomFilter.mightContain(2L)).thenReturn(false);

        assertNull(cacheService.queryById(2L, databaseFallback));

        Mockito.verifyNoInteractions(redisTemplate, databaseFallback);
    }

    @Test
    void redisHitWarmsLocalCache() {
        Shop expected = shop();
        Mockito.when(valueOperations.get("cache:shop:2"))
                .thenReturn(JSONUtil.toJsonStr(expected));

        Shop result = cacheService.queryById(2L, databaseFallback);

        assertEquals(expected.getName(), result.getName());
        Mockito.verify(localCache).put(2L, result);
        Mockito.verifyNoInteractions(redissonClient, databaseFallback);
    }

    @Test
    void cachedNullSkipsLockAndDatabase() {
        Mockito.when(valueOperations.get("cache:shop:2")).thenReturn("");

        assertNull(cacheService.queryById(2L, databaseFallback));

        Mockito.verifyNoInteractions(redissonClient, databaseFallback);
    }

    @Test
    void distributedLockDoubleChecksRedisBeforeDatabase() throws InterruptedException {
        Mockito.when(valueOperations.get("cache:shop:2"))
                .thenReturn(null)
                .thenReturn(JSONUtil.toJsonStr(shop()));
        Mockito.when(lock.tryLock(500L, 10_000L, TimeUnit.MILLISECONDS)).thenReturn(true);
        Mockito.when(lock.isHeldByCurrentThread()).thenReturn(true);

        Shop result = cacheService.queryById(2L, databaseFallback);

        assertEquals(2L, result.getId());
        Mockito.verifyNoInteractions(databaseFallback);
        Mockito.verify(lock).unlock();
    }

    @Test
    void databaseMissWritesShortLivedNullMarker() throws InterruptedException {
        Mockito.when(valueOperations.get("cache:shop:2")).thenReturn(null);
        Mockito.when(lock.tryLock(500L, 10_000L, TimeUnit.MILLISECONDS)).thenReturn(true);
        Mockito.when(lock.isHeldByCurrentThread()).thenReturn(true);
        Mockito.when(databaseFallback.apply(2L)).thenReturn(null);

        assertNull(cacheService.queryById(2L, databaseFallback));

        Mockito.verify(valueOperations).set(
                Mockito.eq("cache:shop:2"),
                Mockito.eq(""),
                Mockito.longThat(ttl -> ttl >= 120L && ttl <= 150L),
                Mockito.eq(TimeUnit.SECONDS));
        Mockito.verify(localCache).invalidate(2L);
    }

    @Test
    void databaseHitPopulatesBothCacheLevels() throws InterruptedException {
        Shop expected = shop();
        Mockito.when(valueOperations.get("cache:shop:2")).thenReturn(null);
        Mockito.when(lock.tryLock(500L, 10_000L, TimeUnit.MILLISECONDS)).thenReturn(true);
        Mockito.when(lock.isHeldByCurrentThread()).thenReturn(true);
        Mockito.when(databaseFallback.apply(2L)).thenReturn(expected);

        assertSame(expected, cacheService.queryById(2L, databaseFallback));

        Mockito.verify(valueOperations).set(
                "cache:shop:2", JSONUtil.toJsonStr(expected), 30L, TimeUnit.MINUTES);
        Mockito.verify(localCache).put(2L, expected);
    }

    private Shop shop() {
        return new Shop().setId(2L).setName("shop-2");
    }
}
