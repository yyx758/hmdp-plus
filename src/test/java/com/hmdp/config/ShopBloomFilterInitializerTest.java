package com.hmdp.config;

import com.hmdp.cache.ShopBloomFilter;
import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;

class ShopBloomFilterInitializerTest {

    @Test
    void initializesAllPersistedShopIds() {
        IShopService shopService = Mockito.mock(IShopService.class);
        ShopBloomFilter bloomFilter = Mockito.mock(ShopBloomFilter.class);
        Mockito.when(shopService.list()).thenReturn(Arrays.asList(
                new Shop().setId(1L), new Shop().setId(2L)));

        new ShopBloomFilterInitializer(shopService, bloomFilter).run(null);

        Mockito.verify(bloomFilter).initialize(Arrays.asList(1L, 2L));
    }
}
