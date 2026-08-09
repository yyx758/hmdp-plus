package com.hmdp.service;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.cache.ShopCacheInvalidationPublisher;
import com.hmdp.cache.ShopLocalCache;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metrics;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.GeoOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShopServiceQueryTest {

    private ShopServiceImpl shopService;
    private ShopMapper shopMapper;
    private StringRedisTemplate redisTemplate;
    private GeoOperations<String, String> geoOperations;
    private CacheClient cacheClient;
    private ShopLocalCache shopLocalCache;
    private ShopCacheInvalidationPublisher cacheInvalidationPublisher;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        shopService = new ShopServiceImpl();
        shopMapper = Mockito.mock(ShopMapper.class);
        redisTemplate = Mockito.mock(StringRedisTemplate.class);
        geoOperations = Mockito.mock(GeoOperations.class);
        cacheClient = Mockito.mock(CacheClient.class);
        shopLocalCache = Mockito.mock(ShopLocalCache.class);
        cacheInvalidationPublisher = Mockito.mock(ShopCacheInvalidationPublisher.class);

        Mockito.when(redisTemplate.opsForGeo()).thenReturn(geoOperations);
        ReflectionTestUtils.setField(shopService, "baseMapper", shopMapper);
        ReflectionTestUtils.setField(shopService, "stringRedisTemplate", redisTemplate);
        ReflectionTestUtils.setField(shopService, "cacheClient", cacheClient);
        ReflectionTestUtils.setField(shopService, "shopLocalCache", shopLocalCache);
        ReflectionTestUtils.setField(shopService, "shopCacheInvalidationPublisher", cacheInvalidationPublisher);
    }

    @Test
    void returnsShopFromCaffeineWithoutReadingRedisL2() {
        Shop cached = new Shop().setId(1L).setName("local-cache");
        Mockito.when(shopLocalCache.get(1L)).thenReturn(cached);

        Result result = shopService.queryById(1L);

        assertTrue(result.getSuccess());
        assertEquals(cached, result.getData());
        Mockito.verifyNoInteractions(cacheClient);
    }

    @Test
    @SuppressWarnings("unchecked")
    void fillsCaffeineAfterReadingRedisL2OrDatabase() {
        Shop cached = new Shop().setId(2L).setName("redis-or-db");
        Mockito.when(shopLocalCache.get(2L)).thenReturn(null);
        Mockito.when(cacheClient.queryWithPassThrough(
                        Mockito.eq("cache:shop:"),
                        Mockito.eq(2L),
                        Mockito.eq(Shop.class),
                        Mockito.<Function<Long, Shop>>any(),
                        Mockito.eq(30L),
                        Mockito.eq(TimeUnit.MINUTES)))
                .thenReturn(cached);

        Result result = shopService.queryById(2L);

        assertTrue(result.getSuccess());
        assertEquals(cached, result.getData());
        Mockito.verify(shopLocalCache).put(2L, cached);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void queriesPopularityWithAreaAndStableOrdering() {
        Shop expected = new Shop().setId(3L).setComments(100);
        Mockito.when(shopMapper.selectPage(Mockito.any(Page.class), Mockito.any(Wrapper.class)))
                .thenAnswer(invocation -> {
                    Page<Shop> page = invocation.getArgument(0);
                    page.setRecords(Arrays.asList(expected));
                    return page;
                });

        Result result = shopService.queryShopByType(1, 1, null, null, "comments", "大关");

        assertTrue(result.getSuccess());
        assertEquals(Arrays.asList(expected), result.getData());
        ArgumentCaptor<Wrapper<Shop>> wrapperCaptor = ArgumentCaptor.forClass(Wrapper.class);
        Mockito.verify(shopMapper).selectPage(Mockito.any(Page.class), wrapperCaptor.capture());
        String sql = wrapperCaptor.getValue().getSqlSegment();
        assertTrue(sql.contains("type_id"));
        assertTrue(sql.contains("area"));
        assertTrue(sql.contains("comments"));
        assertTrue(sql.contains("id"));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void filtersNearbyShopsByAreaBeforePaging() {
        Mockito.when(redisTemplate.hasKey("shop:geo:1")).thenReturn(true);
        GeoResults<RedisGeoCommands.GeoLocation<String>> geoResults = new GeoResults<>(Arrays.asList(
                new GeoResult<>(
                        new RedisGeoCommands.GeoLocation<>("1", null),
                        new Distance(0.3, Metrics.KILOMETERS)),
                new GeoResult<>(
                        new RedisGeoCommands.GeoLocation<>("2", null),
                        new Distance(0.2, Metrics.KILOMETERS))
        ));
        Mockito.when(geoOperations.search(
                        Mockito.eq("shop:geo:1"),
                        Mockito.any(GeoReference.class),
                        Mockito.any(Distance.class),
                        Mockito.any(RedisGeoCommands.GeoSearchCommandArgs.class)))
                .thenReturn(geoResults);
        Shop nearby = new Shop().setId(2L).setArea("大关");
        Mockito.when(shopMapper.selectList(Mockito.any(Wrapper.class)))
                .thenReturn(Arrays.asList(nearby));

        Result result = shopService.queryShopByType(
                1, 1, 120.149993, 30.334229, "distance", "大关");

        assertTrue(result.getSuccess());
        List<Shop> shops = (List<Shop>) result.getData();
        assertEquals(1, shops.size());
        assertEquals(2L, shops.get(0).getId());
        assertEquals(200D, shops.get(0).getDistance(), 0.001D);
        ArgumentCaptor<Wrapper<Shop>> wrapperCaptor = ArgumentCaptor.forClass(Wrapper.class);
        Mockito.verify(shopMapper).selectList(wrapperCaptor.capture());
        assertTrue(wrapperCaptor.getValue().getSqlSegment().contains("area"));
    }

    @Test
    void rejectsUnsupportedSort() {
        Result result = shopService.queryShopByType(
                1, 1, null, null, "sold", null);

        assertFalse(result.getSuccess());
        assertEquals("不支持的排序方式", result.getErrorMsg());
        Mockito.verifyNoInteractions(shopMapper);
    }

    @Test
    void savesShopAndAddsGeoMember() {
        Shop shop = new Shop()
                .setId(10L)
                .setTypeId(1L)
                .setX(120.15)
                .setY(30.33);
        Mockito.when(shopMapper.insert(shop)).thenReturn(1);

        Result result = shopService.saveShop(shop);

        assertTrue(result.getSuccess());
        Mockito.verify(cacheInvalidationPublisher).publish(10L);
        Mockito.verify(geoOperations).add(
                Mockito.eq("shop:geo:1"),
                Mockito.any(org.springframework.data.geo.Point.class),
                Mockito.eq("10"));
    }
}
