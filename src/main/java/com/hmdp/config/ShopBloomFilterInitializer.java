package com.hmdp.config;

import com.hmdp.cache.ShopBloomFilter;
import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Initializes the shared shop Bloom filter from the database. */
@Slf4j
@Component
public class ShopBloomFilterInitializer implements ApplicationRunner {

    private final IShopService shopService;
    private final ShopBloomFilter shopBloomFilter;

    public ShopBloomFilterInitializer(IShopService shopService, ShopBloomFilter shopBloomFilter) {
        this.shopService = shopService;
        this.shopBloomFilter = shopBloomFilter;
    }

    @Override
    public void run(ApplicationArguments args) {
        try {
            List<Shop> shops = shopService.list();
            List<Long> shopIds = shops == null
                    ? Collections.emptyList()
                    : shops.stream()
                            .map(Shop::getId)
                            .filter(java.util.Objects::nonNull)
                            .collect(Collectors.toList());
            shopBloomFilter.initialize(shopIds);
        } catch (RuntimeException e) {
            log.error("Unable to initialize shop Bloom filter; reads will temporarily fail open", e);
        }
    }
}
