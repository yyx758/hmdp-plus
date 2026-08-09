package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.annotation.CacheConsistencyLock;
import com.hmdp.cache.ShopCacheInvalidationPublisher;
import com.hmdp.cache.ShopLocalCache;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.enums.CacheLockMode;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShopServiceImpl.class);
    private static final String SORT_BY_DISTANCE = "distance";
    private static final String SORT_BY_COMMENTS = "comments";
    private static final String SORT_BY_SCORE = "score";
    private static final double SHOP_SEARCH_RADIUS_METERS = 5000D;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Resource
    private ShopLocalCache shopLocalCache;

    @Resource
    private ShopCacheInvalidationPublisher shopCacheInvalidationPublisher;

    @Override
    @CacheConsistencyLock(name = "shop", key = "#p0", mode = CacheLockMode.READ)
    public Result queryById(Long id) {
        Shop localCacheHit = shopLocalCache.get(id);
        if (localCacheHit != null) {
            return Result.ok(localCacheHit);
        }

        // 解决缓存穿透
        Shop shop = cacheClient
                .queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 互斥锁解决缓存击穿
        // Shop shop = cacheClient
        //         .queryWithMutex(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 逻辑过期解决缓存击穿
        // Shop shop = cacheClient
        //         .queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, 20L, TimeUnit.SECONDS);

        if (shop == null) {
            return Result.fail("店铺不存在！");
        }
        // 7.返回
        shopLocalCache.put(id, shop);
        return Result.ok(shop);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Result saveShop(Shop shop) {
        if (shop == null) {
            return Result.fail("店铺信息不能为空");
        }
        if (!save(shop)) {
            return Result.fail("店铺保存失败");
        }
        syncGeoIndex(null, shop);
        shopCacheInvalidationPublisher.publish(shop.getId());
        return Result.ok(shop.getId());
    }

    @Override
    @CacheConsistencyLock(name = "shop", key = "#p0.id", mode = CacheLockMode.WRITE)
    @Transactional(rollbackFor = Exception.class)
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        Shop oldShop = getById(id);
        if (oldShop == null) {
            return Result.fail("店铺不存在");
        }
        if (!updateById(shop)) {
            return Result.fail("店铺更新失败");
        }
        Shop updatedShop = getById(id);
        syncGeoIndex(oldShop, updatedShop);
        shopCacheInvalidationPublisher.publish(id);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(
            Integer typeId,
            Integer current,
            Double x,
            Double y,
            String sortBy,
            String area) {
        Result validationResult = validateShopQuery(typeId, current, x, y, sortBy);
        if (validationResult != null) {
            return validationResult;
        }

        String normalizedSort = StrUtil.blankToDefault(sortBy, SORT_BY_DISTANCE).toLowerCase(Locale.ROOT);
        String normalizedArea = StrUtil.trimToNull(area);
        if (!SORT_BY_DISTANCE.equals(normalizedSort)) {
            return queryShopsFromDatabase(typeId, current, normalizedSort, normalizedArea);
        }
        if (x == null && y == null) {
            return queryShopsFromDatabase(typeId, current, "", normalizedArea);
        }

        return queryShopsByDistance(typeId, current, x, y, normalizedArea);
    }

    @Override
    public Result queryShopAreas(Integer typeId) {
        if (typeId == null || typeId <= 0) {
            return Result.fail("店铺类型不正确");
        }
        SortedSet<String> areas = new TreeSet<>();
        query().eq("type_id", typeId)
                .isNotNull("area")
                .ne("area", "")
                .list()
                .stream()
                .map(Shop::getArea)
                .filter(StrUtil::isNotBlank)
                .map(String::trim)
                .forEach(areas::add);
        return Result.ok(new ArrayList<>(areas));
    }

    private Result queryShopsFromDatabase(
            Integer typeId,
            Integer current,
            String sortBy,
            String area) {
        com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper<Shop> query =
                query()
                        .eq("type_id", typeId)
                        .eq(StrUtil.isNotBlank(area), "area", area);
        if (SORT_BY_COMMENTS.equals(sortBy)) {
            query.orderByDesc("comments");
        } else if (SORT_BY_SCORE.equals(sortBy)) {
            query.orderByDesc("score");
        }
        query.orderByAsc("id");
        Page<Shop> page = query.page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
        return Result.ok(page.getRecords());
    }

    private Result queryShopsByDistance(
            Integer typeId,
            Integer current,
            Double x,
            Double y,
            String area) {
        String key = SHOP_GEO_KEY + typeId;
        try {
            Boolean hasGeoIndex = stringRedisTemplate.hasKey(key);
            if (!Boolean.TRUE.equals(hasGeoIndex)) {
                boolean hasDatabaseShops = query().eq("type_id", typeId).count() > 0;
                return hasDatabaseShops
                        ? Result.fail("附近商户索引暂不可用，请稍后重试")
                        : Result.ok(Collections.emptyList());
            }
            GeoResults<RedisGeoCommands.GeoLocation<String>> results =
                    stringRedisTemplate.opsForGeo().search(
                            key,
                            GeoReference.fromCoordinate(x, y),
                            new Distance(SHOP_SEARCH_RADIUS_METERS / 1000D, Metrics.KILOMETERS),
                            RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs()
                                    .includeDistance()
                                    .sortAscending()
                    );
            if (results == null || results.getContent().isEmpty()) {
                return Result.ok(Collections.emptyList());
            }
            return pageDistanceResults(results.getContent(), current, area);
        } catch (RuntimeException e) {
            LOGGER.error("查询附近商户失败，key=" + key, e);
            return Result.fail("附近商户服务暂不可用，请稍后重试");
        }
    }

    private Result pageDistanceResults(
            List<GeoResult<RedisGeoCommands.GeoLocation<String>>> geoResults,
            Integer current,
            String area) {
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> orderedResults =
                new ArrayList<>(geoResults);
        orderedResults.sort(Comparator
                .comparingDouble((GeoResult<RedisGeoCommands.GeoLocation<String>> result) ->
                        result.getDistance().getValue())
                .thenComparingLong(result -> Long.parseLong(result.getContent().getName())));

        List<Long> ids = new ArrayList<>(orderedResults.size());
        Map<String, Distance> distanceMap = new HashMap<>(orderedResults.size());
        Map<Long, Integer> rankMap = new HashMap<>(orderedResults.size());
        for (int index = 0; index < orderedResults.size(); index++) {
            GeoResult<RedisGeoCommands.GeoLocation<String>> result = orderedResults.get(index);
            String shopIdStr = result.getContent().getName();
            Long shopId = Long.valueOf(shopIdStr);
            ids.add(shopId);
            distanceMap.put(shopIdStr, result.getDistance());
            rankMap.put(shopId, index);
        }

        List<Shop> shops = query()
                .in("id", ids)
                .eq(StrUtil.isNotBlank(area), "area", area)
                .list();
        shops.sort(Comparator.comparingInt(shop -> rankMap.get(shop.getId())));
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue() * 1000D);
        }

        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        if (from >= shops.size()) {
            return Result.ok(Collections.emptyList());
        }
        int to = Math.min(from + SystemConstants.DEFAULT_PAGE_SIZE, shops.size());
        return Result.ok(new ArrayList<>(shops.subList(from, to)));
    }

    private Result validateShopQuery(
            Integer typeId,
            Integer current,
            Double x,
            Double y,
            String sortBy) {
        if (typeId == null || typeId <= 0) {
            return Result.fail("店铺类型不正确");
        }
        if (current == null || current <= 0) {
            return Result.fail("页码必须大于0");
        }
        String normalizedSort = StrUtil.blankToDefault(sortBy, SORT_BY_DISTANCE).toLowerCase(Locale.ROOT);
        if (!SORT_BY_DISTANCE.equals(normalizedSort)
                && !SORT_BY_COMMENTS.equals(normalizedSort)
                && !SORT_BY_SCORE.equals(normalizedSort)) {
            return Result.fail("不支持的排序方式");
        }
        if ((x == null) != (y == null)) {
            return Result.fail("经纬度必须同时提供");
        }
        if (x != null && (x < -180D || x > 180D || y < -90D || y > 90D)) {
            return Result.fail("经纬度不正确");
        }
        return null;
    }

    private void syncGeoIndex(Shop oldShop, Shop newShop) {
        try {
            if (oldShop != null && oldShop.getTypeId() != null) {
                stringRedisTemplate.opsForGeo().remove(
                        SHOP_GEO_KEY + oldShop.getTypeId(),
                        oldShop.getId().toString());
            }
            if (hasGeoCoordinates(newShop)) {
                stringRedisTemplate.opsForGeo().add(
                        SHOP_GEO_KEY + newShop.getTypeId(),
                        new Point(newShop.getX(), newShop.getY()),
                        newShop.getId().toString());
            }
        } catch (RuntimeException e) {
            Long shopId = newShop == null ? null : newShop.getId();
            LOGGER.error("同步店铺GEO索引失败，shopId=" + shopId, e);
        }
    }

    private boolean hasGeoCoordinates(Shop shop) {
        return shop != null
                && shop.getId() != null
                && shop.getTypeId() != null
                && shop.getX() != null
                && shop.getY() != null;
    }
}
