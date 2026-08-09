package com.hmdp.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.concurrent.TimeUnit;

/**
 * JVM-local L1 cache for read-mostly shop details. Redis remains the shared L2 cache.
 */
@Component
@Slf4j
public class ShopLocalCache {

    private final Cache<Long, Shop> cache;
    private final boolean enabled;

    public ShopLocalCache(
            @Value("${hmdp.cache.shop.local.enabled:true}") boolean enabled,
            @Value("${hmdp.cache.shop.local.maximum-size:10000}") long maximumSize,
            @Value("${hmdp.cache.shop.local.expire-after-write-seconds:60}") long expireAfterWriteSeconds) {
        Assert.isTrue(maximumSize > 0, "Shop local cache maximum size must be positive");
        Assert.isTrue(expireAfterWriteSeconds > 0,
                "Shop local cache expire-after-write seconds must be positive");
        this.enabled = enabled;
        this.cache = Caffeine.newBuilder()
                .maximumSize(maximumSize)
                .expireAfterWrite(expireAfterWriteSeconds, TimeUnit.SECONDS)
                .build();
        log.info("Shop Caffeine L1 cache enabled={}, maximumSize={}, expireAfterWriteSeconds={}",
                enabled, maximumSize, expireAfterWriteSeconds);
    }

    public Shop get(Long shopId) {
        return !enabled || shopId == null ? null : cache.getIfPresent(shopId);
    }

    public void put(Long shopId, Shop shop) {
        if (enabled && shopId != null && shop != null) {
            cache.put(shopId, shop);
        }
    }

    public void invalidate(Long shopId) {
        if (enabled && shopId != null) {
            cache.invalidate(shopId);
        }
    }
}
