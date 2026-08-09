package com.hmdp.cache;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collection;

import static com.hmdp.utils.RedisConstants.SECKILL_VOUCHER_BLOOM_FILTER;

/**
 * Redis 共享布隆过滤器。初始化失败时采用 fail-open，避免合法请求被误拦截。
 */
@Slf4j
@Component
public class SeckillVoucherBloomFilter {

    private final RBloomFilter<Long> bloomFilter;
    private final long expectedInsertions;
    private final double falseProbability;
    private volatile boolean ready;

    public SeckillVoucherBloomFilter(
            RedissonClient redissonClient,
            @Value("${hmdp.cache.seckill-voucher.bloom.expected-insertions:100000}")
            long expectedInsertions,
            @Value("${hmdp.cache.seckill-voucher.bloom.false-probability:0.01}")
            double falseProbability) {
        this.bloomFilter = redissonClient.getBloomFilter(SECKILL_VOUCHER_BLOOM_FILTER);
        this.expectedInsertions = expectedInsertions;
        this.falseProbability = falseProbability;
    }

    public void initialize(Collection<Long> voucherIds) {
        try {
            bloomFilter.tryInit(expectedInsertions, falseProbability);
            if (voucherIds != null) {
                voucherIds.stream().filter(java.util.Objects::nonNull).forEach(bloomFilter::add);
            }
            ready = true;
            log.info("秒杀券布隆过滤器初始化完成，合法ID数量={}",
                    voucherIds == null ? 0 : voucherIds.size());
        } catch (RuntimeException e) {
            ready = false;
            log.error("秒杀券布隆过滤器初始化失败，将暂时放行请求到Redis", e);
        }
    }

    public void put(Long voucherId) {
        if (voucherId == null) {
            return;
        }
        try {
            ensureInitialized();
            bloomFilter.add(voucherId);
            ready = true;
        } catch (RuntimeException e) {
            ready = false;
            log.error("新增秒杀券ID写入布隆过滤器失败，voucherId={}", voucherId, e);
        }
    }

    public boolean mightContain(Long voucherId) {
        if (voucherId == null) {
            return false;
        }
        if (!ready) {
            return true;
        }
        try {
            return bloomFilter.contains(voucherId);
        } catch (RuntimeException e) {
            ready = false;
            log.error("查询秒杀券布隆过滤器失败，将放行到Redis，voucherId={}", voucherId, e);
            return true;
        }
    }

    private void ensureInitialized() {
        if (!ready) {
            bloomFilter.tryInit(expectedInsertions, falseProbability);
        }
    }
}
