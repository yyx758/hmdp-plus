package com.hmdp.cache;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collection;

import static com.hmdp.utils.RedisConstants.SHOP_BLOOM_FILTER;

/** Redis-backed Bloom filter for valid shop ids. */
@Slf4j
@Component
public class ShopBloomFilter {

    private final RBloomFilter<Long> bloomFilter;
    /*
     *expectedInsertions,expectedInsertions
     *会根据这两个参数计算 Bloom Filter 所需 bit 数量、Hash 次数等配置，并把相关配置存储到 Redis。
     */
    private final long expectedInsertions;
    private final double falseProbability;
    private volatile boolean ready;//用于标记布隆过滤器是否初始化成功,初始化成功了才进入布隆过滤器,否则直接进入Cache,Redis,MySQL

    public ShopBloomFilter(
            RedissonClient redissonClient,
            @Value("${hmdp.cache.shop.bloom.expected-insertions:100000}") long expectedInsertions,
            @Value("${hmdp.cache.shop.bloom.false-probability:0.01}") double falseProbability) {
        this.bloomFilter = redissonClient.getBloomFilter(SHOP_BLOOM_FILTER);//指定布隆过滤器的名称
        this.expectedInsertions = expectedInsertions;//预计元素数量
        this.falseProbability = falseProbability;//允许的误判概率
    }
/*
* 初始化
* 把传入的shopIds集合都通过布隆过滤器
* 成功了就把ready设置为true
* 否则ready为false
* */
    public void initialize(Collection<Long> shopIds) {
        try {
            bloomFilter.tryInit(expectedInsertions, falseProbability);
            if (shopIds != null) {
                shopIds.stream()
                        .filter(java.util.Objects::nonNull)
                        .forEach(bloomFilter::add);
            }
            ready = true;
            log.info("Shop Bloom filter initialized, validShopCount={}",
                    shopIds == null ? 0 : shopIds.size());
        } catch (RuntimeException e) {
            ready = false;
            log.error("Shop Bloom filter initialization failed; shop reads will fail open", e);
        }
    }

    public void put(Long shopId) {
        if (shopId == null) {
            return;
        }
        boolean initialized = ready;//记录此时是否初始化
        try {
            ensureInitialized();//如果为未初始化就再次尝试初始化
            bloomFilter.add(shopId);//核心方法,底层是bit和hash,把传入的元素经过多次hash进行标记,用于后续判断是否可能存在
            // Adding one id is not equivalent to completing the full database initialization.
            // Keep failing open if startup initialization has not succeeded yet.
            ready = initialized;//恢复原来的初始化标记,这样我们就能知道前面没有全部初始化完成
        } catch (RuntimeException e) {
            ready = false;
            log.error("Failed to add shop id to Bloom filter, shopId={}", shopId, e);
        }
    }

    public boolean mightContain(Long shopId) {
        if (shopId == null) {
            return false;
        }
        /*
        * 这里是fail open思想
        * 布隆过滤器不可用了,我们不默认拒绝,而是继续去缓存去数据库查找,最坏情况也就是多一次查询
        * 如果是fail close
        * 布隆过滤器不可用了,我们默认拒绝,可能会拒绝实际存在的数据,产生业务问题
        * */
        if (!ready) {
            return true;
        }
        try {
            /*
            * bloomFilter.contains方法就是通过bit数组和多次hash来判断是否有可能存在
            * 只要有一个hash的位置为0就不可能存在
            * */
            return bloomFilter.contains(shopId);
        } catch (RuntimeException e) {
            ready = false;
            log.error("Failed to query shop Bloom filter; shop read will fail open, shopId={}",
                    shopId, e);
            return true;
        }
    }

    private void ensureInitialized() {
        if (!ready) {
            bloomFilter.tryInit(expectedInsertions, falseProbability);
        }
    }
}
