package com.hmdp.cache;

import com.hmdp.entity.Shop;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class ShopLocalCacheTest {

    @Test
    void storesAndInvalidatesShop() {
        ShopLocalCache cache = new ShopLocalCache(true, 10, 60);
        Shop shop = new Shop().setId(1L).setName("cached");

        cache.put(1L, shop);
        assertSame(shop, cache.get(1L));

        cache.invalidate(1L);
        assertNull(cache.get(1L));
    }

    @Test
    void bypassesCaffeineWhenDisabled() {
        ShopLocalCache cache = new ShopLocalCache(false, 10, 60);
        Shop shop = new Shop().setId(1L).setName("not-cached");

        cache.put(1L, shop);

        assertNull(cache.get(1L));
    }
}
