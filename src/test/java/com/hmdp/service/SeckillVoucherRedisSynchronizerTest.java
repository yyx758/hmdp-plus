package com.hmdp.service;

import com.hmdp.cache.SeckillVoucherLocalCache;
import com.hmdp.dto.SeckillVoucherCacheDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SeckillVoucherRedisSynchronizerTest {

    private SeckillVoucherRedisSynchronizer synchronizer;
    private StringRedisTemplate redisTemplate;
    private ValueOperations<String, String> valueOperations;
    private HashOperations<String, Object, Object> hashOperations;
    private SeckillVoucherLocalCache localCache;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redisTemplate = Mockito.mock(StringRedisTemplate.class);
        valueOperations = Mockito.mock(ValueOperations.class);
        hashOperations = Mockito.mock(HashOperations.class);
        localCache = Mockito.mock(SeckillVoucherLocalCache.class);
        Mockito.when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        Mockito.when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        synchronizer = new SeckillVoucherRedisSynchronizer(
                redisTemplate, localCache, 300L, 86400L);
    }

    @Test
    void newVoucherSynchronizesInitialStockAndCompleteMetadata() {
        SeckillVoucher voucher = voucher();
        Voucher voucherInfo = voucherInfo(1);
        long before = System.currentTimeMillis();

        synchronizer.synchronizeNewVoucher(voucher, voucherInfo);

        Mockito.verify(valueOperations).set("seckill:stock:2", "100");
        verifyMetadata(1, before);
        Mockito.verify(redisTemplate).delete("seckill:meta:null:2");
        Mockito.verify(localCache).put(Mockito.argThat(value ->
                value.getVoucherId().equals(2L)
                        && value.getShopId().equals(10L)
                        && value.getStatus().equals(1)));
    }

    @Test
    void metadataUpdateDoesNotOverwriteRealtimeStock() {
        long before = System.currentTimeMillis();

        synchronizer.synchronizeMetadata(voucher(), voucherInfo(2));

        Mockito.verifyNoInteractions(valueOperations);
        verifyMetadata(2, before);
    }

    @Test
    void startupOnlyCreatesMissingStockKey() {
        SeckillVoucher voucher = voucher();
        Voucher voucherInfo = voucherInfo(1);
        Mockito.when(valueOperations.setIfAbsent("seckill:stock:2", "100")).thenReturn(true);

        long before = System.currentTimeMillis();
        boolean created = synchronizer.initializeVoucher(voucher, voucherInfo);

        assertTrue(created);
        Mockito.verify(valueOperations).setIfAbsent("seckill:stock:2", "100");
        verifyMetadata(1, before);
    }

    @Test
    void rollbackCleanupDeletesStockAndMetadataSeparately() {
        synchronizer.deleteVoucher(2L);

        // 两次单 key 删除可用于 Redis Cluster，不依赖两个 key 落在同一 slot。
        Mockito.verify(redisTemplate).delete("seckill:stock:2");
        Mockito.verify(redisTemplate).delete("seckill:meta:2");
        Mockito.verify(redisTemplate).delete("seckill:meta:null:2");
        Mockito.verify(localCache).invalidate(2L);
    }

    private SeckillVoucher voucher() {
        return new SeckillVoucher()
                .setVoucherId(2L)
                .setStock(100)
                .setBeginTime(LocalDateTime.of(2026, 8, 4, 10, 0))
                .setEndTime(LocalDateTime.of(2026, 8, 4, 12, 0));
    }

    private Voucher voucherInfo(int status) {
        return new Voucher()
                .setId(2L)
                .setShopId(10L)
                .setStatus(status);
    }

    @SuppressWarnings("unchecked")
    private void verifyMetadata(int status, long before) {
        LocalDateTime beginTime = LocalDateTime.of(2026, 8, 4, 10, 0);
        LocalDateTime endTime = LocalDateTime.of(2026, 8, 4, 12, 0);
        ArgumentCaptor<Map<Object, Object>> captor = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(hashOperations).putAll(Mockito.eq("seckill:meta:2"), captor.capture());
        Map<Object, Object> metadata = captor.getValue();
        assertEquals("2", metadata.get("voucherId"));
        assertEquals("10", metadata.get("shopId"));
        assertEquals(String.valueOf(toEpochMilli(beginTime)), metadata.get("beginTime"));
        assertEquals(String.valueOf(toEpochMilli(endTime)), metadata.get("endTime"));
        assertEquals(String.valueOf(status), metadata.get("status"));
        assertEquals("2026-08-04 10:00:00", metadata.get("beginTimeReadable"));
        assertEquals("2026-08-04 12:00:00", metadata.get("endTimeReadable"));
        long logicalExpireAt = Long.parseLong(metadata.get("logicalExpireAt").toString());
        assertTrue(logicalExpireAt >= before + TimeUnit.SECONDS.toMillis(300));
        assertTrue(logicalExpireAt <= System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(300));
        Mockito.verify(redisTemplate).expire(
                Mockito.eq("seckill:meta:2"), Mockito.anyLong(), Mockito.eq(TimeUnit.SECONDS));
    }

    private long toEpochMilli(LocalDateTime time) {
        return time.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
}
