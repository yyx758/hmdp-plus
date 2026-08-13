package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.cache.ShopBloomFilter;
import com.hmdp.cache.ShopLocalCache;
import com.hmdp.entity.Shop;
import com.hmdp.service.IShopCacheService;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;

/** Shop read path: Caffeine -> Bloom -> Redis/null marker -> Redisson DCL -> MySQL. */
@Service
public class ShopCacheServiceImpl implements IShopCacheService {

    private final ShopLocalCache localCache;
    private final ShopBloomFilter bloomFilter;
    private final StringRedisTemplate stringRedisTemplate;
    private final RedissonClient redissonClient;
    private final long redisTtlMinutes;
    private final long nullTtlSeconds;
    private final long rebuildWaitMillis;
    private final long rebuildLeaseMillis;

    public ShopCacheServiceImpl(
            ShopLocalCache localCache,
            ShopBloomFilter bloomFilter,
            StringRedisTemplate stringRedisTemplate,
            RedissonClient redissonClient,
            @Value("${hmdp.cache.shop.redis-ttl-minutes:30}") long redisTtlMinutes,
            @Value("${hmdp.cache.shop.null-ttl-seconds:120}") long nullTtlSeconds,
            @Value("${hmdp.cache.shop.rebuild.wait-millis:3000}") long rebuildWaitMillis,
            @Value("${hmdp.cache.shop.rebuild.lease-millis:10000}") long rebuildLeaseMillis) {
        this.localCache = localCache;
        this.bloomFilter = bloomFilter;
        this.stringRedisTemplate = stringRedisTemplate;
        this.redissonClient = redissonClient;
        this.redisTtlMinutes = redisTtlMinutes;
        this.nullTtlSeconds = nullTtlSeconds;
        this.rebuildWaitMillis = rebuildWaitMillis;
        this.rebuildLeaseMillis = rebuildLeaseMillis;
    }

    @Override
    public Shop queryById(Long shopId, Function<Long, Shop> databaseFallback) {
        if (shopId == null) {
            return null;
        }
        /*
        * 从本地Cache查找,要是非空就是缓存命中,直接返回localHit
        * */
        Shop localHit = localCache.get(shopId);
        if (localHit != null) {
            return localHit;
        }
        /*
        * 到这里说明本地缓存没有,先通过布隆过滤器来判断是否有可能存在,可能存在才继续去查找redis
        * */
        if (!bloomFilter.mightContain(shopId)) {
            return null;
        }
        //查找Redis,返货查找结果的状态
        CacheLookup firstLookup = readRedis(shopId);
        if (firstLookup.shop != null) {
            //不为null说明redis里找到了缓存,重新写入Cache,返回这个shop就好了
            localCache.put(shopId, firstLookup.shop);
            return firstLookup.shop;
        }
        //shop为空,nullMarker标记为true,说明数据库里没有这条数据.直接返回null
        if (firstLookup.nullMarker) {
            return null;
        }
        //到这里就是要查找数据库重建两层缓存L1,L2
        //多个线程进入只需要一个线程来进行重建的操作,所以用redisson来加一把锁,只有获取了锁的线程才能进行操作
        RLock lock = redissonClient.getLock(LOCK_SHOP_KEY + shopId);

        boolean locked = false;
        try {
            /*
            * trylock三个参数
            * waitTime→ 最多等多久去抢锁
            * leaseTime→ 抢到锁以后，这把锁最多持有多久
            * TimeUnit->前两个参数的单位,ms,s等
            * */
            locked = lock.tryLock(rebuildWaitMillis, rebuildLeaseMillis, TimeUnit.MILLISECONDS);
            //没有获取到锁
            if (!locked) {
                return readLastChance(shopId);
            }
            /*
            * 这里非常关键,这是双重检查锁的思想
            * 第一次查缓存到你真正拿到锁并不是原子的,中间可能过去了几毫秒、几百毫秒甚至几秒钟，这期间另一个线程可能已经把缓存重建好了。
            * 所以多并发情况下,拿到锁并不意味着就要进行缓存重建,而是再次检查是否存在缓存
            * */
            Shop secondLocalHit = localCache.get(shopId);

            //不为null,说明有其他线程已经重建好了
            if (secondLocalHit != null) {
                return secondLocalHit;
            }
            /**
             * 到这里只能说明当前jvm的Cache还没有重建好,并不能说其他jvm没有进行重建
             * 因为Cache是本机的缓存,而Redis是分布式缓存,可能其他的jvm中某个线程进行了重建,但是他只能更新Redis是分布式缓存和自己的jvm中的本机缓存
             * 所以还是要先查询Redis缓存,如果Redis中有缓存,说明已经重建好了,写回本机的缓存就好了
             */
            CacheLookup secondLookup = readRedis(shopId);
            if (secondLookup.shop != null) {
                localCache.put(shopId, secondLookup.shop);
                return secondLookup.shop;
            }
            //这里就是说明数据库里没有,直接返回null
            if (secondLookup.nullMarker) {
                return null;
            }

            //调用“外面传进来的数据库查询函数”，把 shopId 传进去，得到一个 Shop。
            Shop shop = databaseFallback.apply(shopId);

            if (shop == null) {
                //数据库为空就在redis里缓存一个空值
                cacheNull(shopId);
                //使本地缓存失效
                localCache.invalidate(shopId);
                return null;
            }
            //写入redis和本地缓存
            cacheShop(shopId, shop);
            return shop;
        } catch (InterruptedException e) {
            //其他线程可能调用interrupted方法,捕获之后通常要恢复中断标记
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while acquiring shop cache rebuild lock, shopId=" + shopId, e);
        } finally {
            //释放锁
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private Shop readLastChance(Long shopId) {
        Shop localHit = localCache.get(shopId);
        if (localHit != null) {
            return localHit;
        }
        CacheLookup lookup = readRedis(shopId);
        if (lookup.shop != null) {
            localCache.put(shopId, lookup.shop);
            return lookup.shop;
        }
        if (lookup.nullMarker) {
            return null;
        }
        throw new IllegalStateException(
                "Timed out waiting for shop cache rebuild, shopId=" + shopId);
    }

    /*
    * 查找redis,不为null不为"",则表示缓存命中
    * 为""表示命中缓存的空值,不用在向下查找
    * 为null表示Redis也没命中,不知道数据库里是否有该数据,后续继续查找MySQL
    * */
    private CacheLookup readRedis(Long shopId) {
        String json = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + shopId);
        if (StrUtil.isNotBlank(json)) {
            return CacheLookup.hit(JSONUtil.toBean(json, Shop.class));
        }
        return json == null ? CacheLookup.miss() : CacheLookup.nullMarker();
    }

    /*
    * Redis未命中时使用,作用是把从数据库查找到的shop写入Redis和Cache
    * */
    private void cacheShop(Long shopId, Shop shop) {
        stringRedisTemplate.opsForValue().set(
                CACHE_SHOP_KEY + shopId,
                JSONUtil.toJsonStr(shop),
                redisTtlMinutes,
                TimeUnit.MINUTES);
        localCache.put(shopId, shop);
    }

    private void cacheNull(Long shopId) {
        //jitter表示抖动,这是防止缓存雪崩的一种手段
        long jitterSeconds = ThreadLocalRandom.current().nextLong(0, 31);
        //临时缓存空值
        stringRedisTemplate.opsForValue().set(
                CACHE_SHOP_KEY + shopId,
                "",
                nullTtlSeconds + jitterSeconds,
                TimeUnit.SECONDS);
    }

    /*
    * 这个类来表示缓存命中的情况
    * shop不为null,表示命中,用 new CacheLookup(shop, false);表示
    * shop为null
    * 1.可能是命中了临时缓存的空值,用 new CacheLookup(null, false);表示
    * 2.没命中缓存,需要继续去Redis查询,用  CacheLookup(null, false);表示
    * */
    private static final class CacheLookup {
        private final Shop shop;
        private final boolean nullMarker;

        private CacheLookup(Shop shop, boolean nullMarker) {
            this.shop = shop;
            this.nullMarker = nullMarker;
        }

        private static CacheLookup hit(Shop shop) {
            return new CacheLookup(shop, false);
        }

        private static CacheLookup nullMarker() {
            return new CacheLookup(null, true);
        }

        private static CacheLookup miss() {
            return new CacheLookup(null, false);
        }
    }
}
