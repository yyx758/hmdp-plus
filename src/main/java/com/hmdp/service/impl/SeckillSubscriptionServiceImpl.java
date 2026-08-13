package com.hmdp.service.impl;

import com.hmdp.config.SeckillMarketingProperties;
import com.hmdp.dto.VoucherSubscribeStatusDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.enums.VoucherSubscribeStatus;
import com.hmdp.exception.BusinessException;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.service.ISeckillSubscriptionService;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.SECKILL_SUBSCRIBE_QUEUE_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_SUBSCRIBE_STATUS_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_SUBSCRIBE_USER_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_VOUCHER_META_KEY;

@Service
public class SeckillSubscriptionServiceImpl implements ISeckillSubscriptionService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private SeckillVoucherMapper seckillVoucherMapper;

    @Resource
    private SeckillMarketingProperties properties;

    @Override
    public void subscribe(Long voucherId, Long userId) {
        long ttlSeconds = resolveTtlSeconds(voucherId);
        String userIdText = userId.toString();
        if (Boolean.TRUE.equals(stringRedisTemplate.opsForSet()
                .isMember(orderKey(voucherId), userIdText))) {
            updateStatus(voucherId, userId, VoucherSubscribeStatus.ISSUED, ttlSeconds);
            return;
        }

        Long added = stringRedisTemplate.opsForSet().add(subscriberKey(voucherId), userIdText);
        if (Long.valueOf(1L).equals(added)) {
            stringRedisTemplate.opsForZSet().add(
                    queueKey(voucherId), userIdText, System.currentTimeMillis());
        }
        expireSubscriptionKeys(voucherId, ttlSeconds);
        updateStatus(voucherId, userId, VoucherSubscribeStatus.SUBSCRIBED, ttlSeconds);
    }

    @Override
    public void unsubscribe(Long voucherId, Long userId) {
        validateIdentity(voucherId, userId);
        String userIdText = userId.toString();
        stringRedisTemplate.opsForSet().remove(subscriberKey(voucherId), userIdText);
        stringRedisTemplate.opsForZSet().remove(queueKey(voucherId), userIdText);
        if (Boolean.TRUE.equals(stringRedisTemplate.opsForSet()
                .isMember(orderKey(voucherId), userIdText))) {
            updateStatus(
                    voucherId, userId, VoucherSubscribeStatus.ISSUED,
                    resolveTtlSecondsOrDefault(voucherId));
        } else {
            stringRedisTemplate.opsForHash().delete(statusKey(voucherId), userIdText);
        }
    }

    @Override
    public int getSubscribeStatus(Long voucherId, Long userId) {
        validateIdentity(voucherId, userId);
        Object cached = stringRedisTemplate.opsForHash()
                .get(statusKey(voucherId), userId.toString());
        if (cached != null) {
            return Integer.parseInt(cached.toString());
        }
        if (Boolean.TRUE.equals(stringRedisTemplate.opsForSet()
                .isMember(orderKey(voucherId), userId.toString()))) {
            return VoucherSubscribeStatus.ISSUED.getCode();
        }
        if (Boolean.TRUE.equals(stringRedisTemplate.opsForSet()
                .isMember(subscriberKey(voucherId), userId.toString()))) {
            return VoucherSubscribeStatus.SUBSCRIBED.getCode();
        }
        return VoucherSubscribeStatus.UNSUBSCRIBED.getCode();
    }

    @Override
    public List<VoucherSubscribeStatusDTO> getSubscribeStatusBatch(
            Collection<Long> voucherIds, Long userId) {
        if (voucherIds == null || voucherIds.isEmpty()) {
            return Collections.emptyList();
        }
        List<VoucherSubscribeStatusDTO> result = new ArrayList<>();
        for (Long voucherId : new LinkedHashSet<>(voucherIds)) {
            if (voucherId != null) {
                result.add(new VoucherSubscribeStatusDTO(
                        voucherId, getSubscribeStatus(voucherId, userId)));
            }
        }
        return result;
    }

    @Override
    public void markPurchased(VoucherOrder voucherOrder) {
        // 订阅仅保留通知功能，无需维护 Redis 购买状态。
    }

    @Override
    public void clearPurchased(VoucherOrder voucherOrder) {
        // 订阅仅保留通知功能，无需维护 Redis 购买状态。
    }

    @Override
    public Set<Long> listSubscriberUserIds(Long voucherId, int limit) {
        if (voucherId == null || limit <= 0) {
            return Collections.emptySet();
        }
        Set<String> values = stringRedisTemplate.opsForZSet()
                .range(queueKey(voucherId), 0, limit - 1L);
        if (values == null || values.isEmpty()) {
            return Collections.emptySet();
        }
        Set<Long> result = new LinkedHashSet<>();
        for (String value : values) {
            try {
                result.add(Long.valueOf(value));
            } catch (NumberFormatException ignored) {
                // 跳过异常成员，正常订阅链路不受影响。
            }
        }
        return result;
    }

    private long resolveTtlSeconds(Long voucherId) {
        if (voucherId == null) {
            throw new BusinessException("优惠券ID不能为空");
        }
        Long redisTtl = stringRedisTemplate.getExpire(
                SECKILL_VOUCHER_META_KEY + voucherId, TimeUnit.SECONDS);
        if (redisTtl != null && redisTtl > 0) {
            return redisTtl;
        }
        SeckillVoucher voucher = seckillVoucherMapper.selectById(voucherId);
        if (voucher == null || voucher.getEndTime() == null) {
            throw new BusinessException("秒杀活动不存在");
        }
        long seconds = Duration.between(LocalDateTime.now(), voucher.getEndTime()).getSeconds();
        if (seconds <= 0) {
            throw new BusinessException("秒杀活动已经结束");
        }
        return Math.max(1L, seconds + Math.max(0, properties.getSubscriptionGraceSeconds()));
    }

    private long resolveTtlSecondsOrDefault(Long voucherId) {
        try {
            return Math.max(3600L, resolveTtlSeconds(voucherId));
        } catch (BusinessException e) {
            return 3600L;
        }
    }

    private void validateIdentity(Long voucherId, Long userId) {
        if (voucherId == null || userId == null) {
            throw new BusinessException("优惠券ID和用户ID不能为空");
        }
    }

    private void updateStatus(
            Long voucherId,
            Long userId,
            VoucherSubscribeStatus status,
            long ttlSeconds) {
        stringRedisTemplate.opsForHash().put(
                statusKey(voucherId), userId.toString(), String.valueOf(status.getCode()));
        stringRedisTemplate.expire(statusKey(voucherId), ttlSeconds, TimeUnit.SECONDS);
    }

    private void expireSubscriptionKeys(Long voucherId, long ttlSeconds) {
        stringRedisTemplate.expire(subscriberKey(voucherId), ttlSeconds, TimeUnit.SECONDS);
        stringRedisTemplate.expire(queueKey(voucherId), ttlSeconds, TimeUnit.SECONDS);
        stringRedisTemplate.expire(statusKey(voucherId), ttlSeconds, TimeUnit.SECONDS);
    }

    private String orderKey(Long voucherId) {
        return "seckill:order:" + voucherId;
    }

    private String subscriberKey(Long voucherId) {
        return SECKILL_SUBSCRIBE_USER_KEY + voucherId;
    }

    private String queueKey(Long voucherId) {
        return SECKILL_SUBSCRIBE_QUEUE_KEY + voucherId;
    }

    private String statusKey(Long voucherId) {
        return SECKILL_SUBSCRIBE_STATUS_KEY + voucherId;
    }
}
