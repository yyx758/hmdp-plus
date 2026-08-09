package com.hmdp.kafka;

import com.hmdp.annotation.CacheConsistencyLock;
import com.hmdp.cache.ShopCacheInvalidationHandler;
import com.hmdp.enums.CacheLockMode;
import org.springframework.stereotype.Component;

/**
 * Uses the same per-shop distributed write lock as shop updates so a read cannot refill stale
 * data while a Kafka invalidation is being applied.
 */
@Component
public class ShopCacheInvalidationConsumerHandler {

    private final ShopCacheInvalidationHandler invalidationHandler;

    public ShopCacheInvalidationConsumerHandler(
            ShopCacheInvalidationHandler invalidationHandler) {
        this.invalidationHandler = invalidationHandler;
    }

    @CacheConsistencyLock(name = "shop", key = "#p0", mode = CacheLockMode.WRITE)
    public void evict(Long shopId) {
        invalidationHandler.evict(shopId);
    }
}
