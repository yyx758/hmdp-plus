package com.hmdp.service;

import com.hmdp.entity.Shop;

import java.util.function.Function;

public interface IShopCacheService {

    Shop queryById(Long shopId, Function<Long, Shop> databaseFallback);
}
