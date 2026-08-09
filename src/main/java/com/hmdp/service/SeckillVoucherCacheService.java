package com.hmdp.service;

import com.hmdp.cache.SeckillVoucherBloomFilter;
import com.hmdp.cache.SeckillVoucherLocalCache;
import com.hmdp.dto.SeckillVoucherCacheDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOCK_SECKILL_VOUCHER_REBUILD_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_VOUCHER_META_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_VOUCHER_NULL_KEY;

/**
 * 秒杀券活动元数据的组合读取链路：
 * Caffeine -> Bloom -> Redis -> 空值缓存 -> 分布式锁+DCL -> MySQL。
 */
@Slf4j
@Service
public class SeckillVoucherCacheService {

    private final SeckillVoucherLocalCache localCache;
    private final SeckillVoucherBloomFilter bloomFilter;
    private final StringRedisTemplate stringRedisTemplate;
    private final RedissonClient redissonClient;
    private final ISeckillVoucherService seckillVoucherService;
    private final IVoucherService voucherService;
    private final SeckillVoucherRedisSynchronizer redisSynchronizer;
    private final Executor rebuildExecutor;
    private final long nullTtlSeconds;
    private final long rebuildWaitMillis;
    private final long rebuildLeaseMillis;

    public SeckillVoucherCacheService(
            SeckillVoucherLocalCache localCache,
            SeckillVoucherBloomFilter bloomFilter,
            StringRedisTemplate stringRedisTemplate,
            RedissonClient redissonClient,
            ISeckillVoucherService seckillVoucherService,
            IVoucherService voucherService,
            SeckillVoucherRedisSynchronizer redisSynchronizer,
            @Qualifier("seckillVoucherCacheRebuildExecutor") Executor rebuildExecutor,
            @Value("${hmdp.cache.seckill-voucher.null-ttl-seconds:120}") long nullTtlSeconds,
            @Value("${hmdp.cache.seckill-voucher.rebuild.wait-millis:200}") long rebuildWaitMillis,
            @Value("${hmdp.cache.seckill-voucher.rebuild.lease-millis:10000}")
            long rebuildLeaseMillis) {
        this.localCache = localCache;
        this.bloomFilter = bloomFilter;
        this.stringRedisTemplate = stringRedisTemplate;
        this.redissonClient = redissonClient;
        this.seckillVoucherService = seckillVoucherService;
        this.voucherService = voucherService;
        this.redisSynchronizer = redisSynchronizer;
        this.rebuildExecutor = rebuildExecutor;
        this.nullTtlSeconds = nullTtlSeconds;
        this.rebuildWaitMillis = rebuildWaitMillis;
        this.rebuildLeaseMillis = rebuildLeaseMillis;
    }

    public SeckillVoucherCacheDTO queryById(Long voucherId) {
        if (voucherId == null) {
            return null;
        }

        // 第一层：JVM 本地缓存，热点请求不产生额外 Redis 元数据查询。
        SeckillVoucherCacheDTO localHit = localCache.get(voucherId);
        if (localHit != null) {
            return localHit;
        }

        // 第二层防穿透：布隆过滤器只会误判为存在，不会把已加载的合法ID判为不存在。
        if (!bloomFilter.mightContain(voucherId)) {
            return null;
        }

        RedisCacheEntry redisEntry = readRedis(voucherId);
        if (redisEntry != null) {
            localCache.put(redisEntry.data);
            if (redisEntry.isLogicallyExpired()) {
                rebuildAsynchronously(voucherId);
            }
            // 逻辑过期仍返回旧值，由后台单线程重建，避免热点瞬间打到数据库。
            return redisEntry.data;
        }

        if (isNullCached(voucherId)) {
            return null;
        }
        return rebuildSynchronously(voucherId);
    }

    private SeckillVoucherCacheDTO rebuildSynchronously(Long voucherId) {
        RLock lock = redissonClient.getLock(LOCK_SECKILL_VOUCHER_REBUILD_KEY + voucherId);
        boolean locked = false;
        try {
            locked = lock.tryLock(rebuildWaitMillis, rebuildLeaseMillis, TimeUnit.MILLISECONDS);
            if (!locked) {
                // 负载保护：不无限递归或休眠，只做最后一次共享缓存检查。
                RedisCacheEntry lastChance = readRedis(voucherId);
                return lastChance == null ? null : lastChance.data;
            }

            // Double-Check：等待锁期间，其他线程可能已经完成重建。
            SeckillVoucherCacheDTO localHit = localCache.get(voucherId);
            if (localHit != null) {
                return localHit;
            }
            RedisCacheEntry secondCheck = readRedis(voucherId);
            if (secondCheck != null) {
                localCache.put(secondCheck.data);
                return secondCheck.data;
            }
            if (isNullCached(voucherId)) {
                return null;
            }
            return loadFromDatabase(voucherId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("获取秒杀券缓存重建锁被中断，voucherId=" + voucherId, e);
        } finally {
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private void rebuildAsynchronously(Long voucherId) {
        try {
            rebuildExecutor.execute(() -> {
                RLock lock = redissonClient.getLock(LOCK_SECKILL_VOUCHER_REBUILD_KEY + voucherId);
                boolean locked = false;
                try {
                    // 异步线程自己加锁和解锁，符合 Redisson 锁的线程归属规则。
                    locked = lock.tryLock(0, rebuildLeaseMillis, TimeUnit.MILLISECONDS);
                    if (!locked) {
                        return;
                    }
                    RedisCacheEntry secondCheck = readRedis(voucherId);
                    if (secondCheck != null && !secondCheck.isLogicallyExpired()) {
                        return;
                    }
                    loadFromDatabase(voucherId);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (RuntimeException e) {
                    log.error("异步重建秒杀券缓存失败，voucherId={}", voucherId, e);
                } finally {
                    if (locked && lock.isHeldByCurrentThread()) {
                        lock.unlock();
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            // 当前请求仍返回旧值，拒绝重建不会放大为用户请求失败。
            log.warn("秒杀券缓存重建队列已满，本次继续使用旧值，voucherId={}", voucherId);
        }
    }

    private SeckillVoucherCacheDTO loadFromDatabase(Long voucherId) {
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        Voucher voucher = voucherService.getById(voucherId);
        if (seckillVoucher == null || voucher == null) {
            cacheNull(voucherId);
            localCache.invalidate(voucherId);
            return null;
        }
        redisSynchronizer.synchronizeMetadata(seckillVoucher, voucher);
        bloomFilter.put(voucherId);
        return toCacheDTO(seckillVoucher, voucher);
    }

    private RedisCacheEntry readRedis(Long voucherId) {
        Map<Object, Object> metadata = stringRedisTemplate.opsForHash()
                .entries(SECKILL_VOUCHER_META_KEY + voucherId);
        if (metadata == null || metadata.isEmpty()) {
            return null;
        }
        try {
            Long beginTime = parseLong(metadata.get("beginTime"));
            Long endTime = parseLong(metadata.get("endTime"));
            Integer status = parseInteger(metadata.get("status"));
            Long logicalExpireAt = parseLong(metadata.get("logicalExpireAt"));
            if (beginTime == null || endTime == null || status == null) {
                return null;
            }
            SeckillVoucherCacheDTO data = new SeckillVoucherCacheDTO()
                    .setVoucherId(voucherId)
                    .setShopId(parseLong(metadata.get("shopId")))
                    .setBeginTime(fromEpochMilli(beginTime))
                    .setEndTime(fromEpochMilli(endTime))
                    .setStatus(status);
            return new RedisCacheEntry(data, logicalExpireAt == null ? 0L : logicalExpireAt);
        } catch (RuntimeException e) {
            log.warn("秒杀券Redis元数据格式异常，将进入加锁重建，voucherId={}", voucherId, e);
            return null;
        }
    }

    private boolean isNullCached(Long voucherId) {
        return Boolean.TRUE.equals(stringRedisTemplate.hasKey(SECKILL_VOUCHER_NULL_KEY + voucherId));
    }

    private void cacheNull(Long voucherId) {
        long jitterSeconds = ThreadLocalRandom.current().nextLong(0, 31);
        stringRedisTemplate.opsForValue().set(
                SECKILL_VOUCHER_NULL_KEY + voucherId,
                "1",
                nullTtlSeconds + jitterSeconds,
                TimeUnit.SECONDS);
    }

    private SeckillVoucherCacheDTO toCacheDTO(SeckillVoucher seckillVoucher, Voucher voucher) {
        return new SeckillVoucherCacheDTO()
                .setVoucherId(seckillVoucher.getVoucherId())
                .setShopId(voucher.getShopId())
                .setBeginTime(seckillVoucher.getBeginTime())
                .setEndTime(seckillVoucher.getEndTime())
                .setStatus(voucher.getStatus());
    }

    private LocalDateTime fromEpochMilli(long epochMilli) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), ZoneId.systemDefault());
    }

    private Long parseLong(Object value) {
        return value == null ? null : Long.valueOf(value.toString());
    }

    private Integer parseInteger(Object value) {
        return value == null ? null : Integer.valueOf(value.toString());
    }

    private static class RedisCacheEntry {
        private final SeckillVoucherCacheDTO data;
        private final long logicalExpireAt;

        private RedisCacheEntry(SeckillVoucherCacheDTO data, long logicalExpireAt) {
            this.data = data;
            this.logicalExpireAt = logicalExpireAt;
        }

        private boolean isLogicallyExpired() {
            return logicalExpireAt <= System.currentTimeMillis();
        }
    }
}
