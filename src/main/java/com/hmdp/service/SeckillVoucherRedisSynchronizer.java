package com.hmdp.service;

import com.hmdp.cache.SeckillVoucherLocalCache;
import com.hmdp.dto.SeckillVoucherCacheDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.SECKILL_STOCK_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_VOUCHER_META_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_VOUCHER_NULL_KEY;

/**
 * 将 MySQL 中的秒杀券活动配置同步到 Redis 和本机 Caffeine。
 *
 * <p>库存和活动元数据采用不同策略：创建活动时写入初始库存；普通活动修改只更新
 * 元数据，绝不覆盖正在被 Lua 扣减的实时库存。</p>
 */
@Component
public class SeckillVoucherRedisSynchronizer {

    private static final DateTimeFormatter READABLE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final StringRedisTemplate stringRedisTemplate;
    private final SeckillVoucherLocalCache localCache;
    private final long logicalTtlSeconds;
    private final long staleGraceSeconds;

    public SeckillVoucherRedisSynchronizer(
            StringRedisTemplate stringRedisTemplate,
            SeckillVoucherLocalCache localCache,
            @Value("${hmdp.cache.seckill-voucher.logical-ttl-seconds:300}")
            long logicalTtlSeconds,
            @Value("${hmdp.cache.seckill-voucher.stale-grace-seconds:86400}")
            long staleGraceSeconds) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.localCache = localCache;
        this.logicalTtlSeconds = logicalTtlSeconds;
        this.staleGraceSeconds = staleGraceSeconds;
    }

    /**
     * 新增秒杀券后写入初始库存和活动元数据。
     */
    public void synchronizeNewVoucher(SeckillVoucher voucher, Voucher voucherInfo) {
        validate(voucher, voucherInfo, true);
        stringRedisTemplate.opsForValue().set(
                SECKILL_STOCK_KEY + voucher.getVoucherId(),
                voucher.getStock().toString());
        synchronizeMetadata(voucher, voucherInfo);
    }

    /**
     * 启动恢复只补不存在的库存，但始终用数据库最新值刷新活动元数据并预热 L1。
     *
     * @return true 表示本次补建了 Redis 库存 key
     */
    public boolean initializeVoucher(SeckillVoucher voucher, Voucher voucherInfo) {
        validate(voucher, voucherInfo, true);
        Boolean created = stringRedisTemplate.opsForValue().setIfAbsent(
                SECKILL_STOCK_KEY + voucher.getVoucherId(),
                voucher.getStock().toString());
        synchronizeMetadata(voucher, voucherInfo);
        return Boolean.TRUE.equals(created);
    }

    /**
     * 修改活动配置后刷新 Redis 元数据和本机 L1，不覆盖 Redis 实时库存。
     */
    public void synchronizeMetadata(SeckillVoucher voucher, Voucher voucherInfo) {
        validate(voucher, voucherInfo, false);
        long nowMillis = System.currentTimeMillis();
        String metadataKey = SECKILL_VOUCHER_META_KEY + voucher.getVoucherId();
        Map<Object, Object> metadata = new HashMap<>();
        metadata.put("voucherId", String.valueOf(voucher.getVoucherId()));
        if (voucherInfo.getShopId() != null) {
            metadata.put("shopId", String.valueOf(voucherInfo.getShopId()));
        }
        metadata.put("beginTime", String.valueOf(toEpochMilli(voucher.getBeginTime())));
        metadata.put("endTime", String.valueOf(toEpochMilli(voucher.getEndTime())));
        metadata.put("status", String.valueOf(voucherInfo.getStatus()));
        metadata.put("logicalExpireAt",
                String.valueOf(nowMillis + TimeUnit.SECONDS.toMillis(logicalTtlSeconds)));
        metadata.put("beginTimeReadable", voucher.getBeginTime().format(READABLE_TIME_FORMATTER));
        metadata.put("endTimeReadable", voucher.getEndTime().format(READABLE_TIME_FORMATTER));
        stringRedisTemplate.opsForHash().putAll(metadataKey, metadata);

        // 物理TTL必须覆盖活动结束时间，否则L1命中时Redis meta可能已消失，Lua会错误拒绝。
        long secondsUntilEnd = Math.max(
                1L,
                Duration.between(LocalDateTime.now(), voucher.getEndTime()).getSeconds());
        long physicalTtlSeconds = Math.max(
                logicalTtlSeconds + staleGraceSeconds,
                secondsUntilEnd + staleGraceSeconds);
        stringRedisTemplate.expire(metadataKey, physicalTtlSeconds, TimeUnit.SECONDS);
        stringRedisTemplate.delete(SECKILL_VOUCHER_NULL_KEY + voucher.getVoucherId());
        localCache.put(toCacheDTO(voucher, voucherInfo));
    }

    /**
     * 新增事务回滚时清理由该事务创建的缓存投影。
     */
    public void deleteVoucher(Long voucherId) {
        if (voucherId == null) {
            return;
        }
        // 分开删除，兼容多个 key 不在同一 Redis Cluster slot 的情况。
        stringRedisTemplate.delete(SECKILL_STOCK_KEY + voucherId);
        stringRedisTemplate.delete(SECKILL_VOUCHER_META_KEY + voucherId);
        stringRedisTemplate.delete(SECKILL_VOUCHER_NULL_KEY + voucherId);
        localCache.invalidate(voucherId);
    }

    private void validate(SeckillVoucher voucher, Voucher voucherInfo, boolean stockRequired) {
        Assert.notNull(voucher, "秒杀券不能为空");
        Assert.notNull(voucherInfo, "优惠券信息不能为空");
        Assert.notNull(voucher.getVoucherId(), "秒杀券id不能为空");
        Assert.isTrue(voucher.getVoucherId().equals(voucherInfo.getId()),
                "优惠券id与秒杀券id不一致");
        Assert.notNull(voucher.getBeginTime(), "秒杀开始时间不能为空");
        Assert.notNull(voucher.getEndTime(), "秒杀结束时间不能为空");
        Assert.isTrue(voucher.getBeginTime().isBefore(voucher.getEndTime()),
                "秒杀开始时间必须早于结束时间");
        Assert.notNull(voucherInfo.getStatus(), "秒杀券状态不能为空");
        Assert.isTrue(voucherInfo.getStatus() >= 1 && voucherInfo.getStatus() <= 3,
                "秒杀券状态只能是1、2或3");
        if (stockRequired) {
            Assert.notNull(voucher.getStock(), "秒杀券库存不能为空");
            Assert.isTrue(voucher.getStock() >= 0, "秒杀券库存不能小于0");
        }
    }

    private long toEpochMilli(LocalDateTime time) {
        return time.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    private SeckillVoucherCacheDTO toCacheDTO(SeckillVoucher voucher, Voucher voucherInfo) {
        return new SeckillVoucherCacheDTO()
                .setVoucherId(voucher.getVoucherId())
                .setShopId(voucherInfo.getShopId())
                .setBeginTime(voucher.getBeginTime())
                .setEndTime(voucher.getEndTime())
                .setStatus(voucherInfo.getStatus());
    }
}
