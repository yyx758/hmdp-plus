package com.hmdp.cache;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

/**
 * Idempotently evicts a shop from both Caffeine L1 and Redis L2.
 */
@Slf4j
@Component
public class ShopCacheInvalidationHandler {

    private final ShopLocalCache shopLocalCache;
    private final StringRedisTemplate stringRedisTemplate;

    public ShopCacheInvalidationHandler(ShopLocalCache shopLocalCache,
                                        StringRedisTemplate stringRedisTemplate) {
        this.shopLocalCache = shopLocalCache;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void evict(Long shopId) {
        if (shopId == null) {
            return;
        }
        shopLocalCache.invalidate(shopId);
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shopId);
    }

    /**
     * Used by the post-commit fast path. Kafka/outbox delivery remains the durable retry path.
     */
    public void evictBestEffort(Long shopId) {
        try {
            evict(shopId);
        } catch (RuntimeException e) {
            log.error("Failed to evict shop from Redis L2 cache, shopId={}", shopId, e);
        }
    }
}
