package com.hmdp.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.hmdp.dto.SeckillVoucherCacheDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.concurrent.TimeUnit;

/**
 * 秒杀券活动元数据的 JVM L1 缓存。实时库存始终留在 Redis。
 */
@Slf4j
@Component
public class SeckillVoucherLocalCache {

    private final Cache<Long, SeckillVoucherCacheDTO> cache;
    private final boolean enabled;

    public SeckillVoucherLocalCache(
            @Value("${hmdp.cache.seckill-voucher.local.enabled:true}") boolean enabled,
            @Value("${hmdp.cache.seckill-voucher.local.maximum-size:10000}") long maximumSize,
            @Value("${hmdp.cache.seckill-voucher.local.expire-after-write-seconds:30}")
            long expireAfterWriteSeconds) {
        Assert.isTrue(maximumSize > 0, "秒杀券本地缓存容量必须大于0");
        Assert.isTrue(expireAfterWriteSeconds > 0, "秒杀券本地缓存过期时间必须大于0");
        this.enabled = enabled;
        this.cache = Caffeine.newBuilder()
                .maximumSize(maximumSize)
                .expireAfterWrite(expireAfterWriteSeconds, TimeUnit.SECONDS)
                .build();
        log.info("秒杀券Caffeine L1缓存 enabled={}，maximumSize={}，expireAfterWriteSeconds={}",
                enabled, maximumSize, expireAfterWriteSeconds);
    }

    public SeckillVoucherCacheDTO get(Long voucherId) {
        return !enabled || voucherId == null ? null : cache.getIfPresent(voucherId);
    }

    public void put(SeckillVoucherCacheDTO voucher) {
        if (enabled && voucher != null && voucher.getVoucherId() != null) {
            cache.put(voucher.getVoucherId(), voucher);
        }
    }

    public void invalidate(Long voucherId) {
        if (enabled && voucherId != null) {
            cache.invalidate(voucherId);
        }
    }
}
