package com.hmdp.service.impl;

import com.hmdp.config.SeckillMarketingProperties;
import com.hmdp.dto.TopBuyerDTO;
import com.hmdp.entity.User;
import com.hmdp.entity.UserInfo;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.UserInfoMapper;
import com.hmdp.mapper.UserMapper;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.service.ISeckillTopBuyerService;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.SECKILL_TOP_BUYERS_DAILY_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_TOP_BUYER_RECORDED_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_VOUCHER_META_KEY;

@Service
public class SeckillTopBuyerServiceImpl implements ISeckillTopBuyerService {

    private static final DateTimeFormatter DAY_FORMATTER = DateTimeFormatter.BASIC_ISO_DATE;
    private static final DefaultRedisScript<Long> RECORD_SCRIPT =
            loadScript("lua/seckill_top_buyer_record.lua");
    private static final DefaultRedisScript<Long> ROLLBACK_SCRIPT =
            loadScript("lua/seckill_top_buyer_rollback.lua");

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private VoucherMapper voucherMapper;

    @Resource
    private UserMapper userMapper;

    @Resource
    private UserInfoMapper userInfoMapper;

    @Resource
    private SeckillMarketingProperties properties;

    @Override
    public void recordSuccessfulOrder(VoucherOrder voucherOrder) {
        if (!properties.isEnabled() || !isValidOrder(voucherOrder)) {
            return;
        }
        Long shopId = resolveShopId(voucherOrder.getVoucherId());
        if (shopId == null) {
            return;
        }
        String day = resolveOrderTime(voucherOrder).toLocalDate().format(DAY_FORMATTER);
        long ttlMillis = TimeUnit.DAYS.toMillis(
                Math.max(1, properties.getTopBuyerRetentionDays()));
        String hashTag = "{" + shopId + "}";
        stringRedisTemplate.execute(
                RECORD_SCRIPT,
                java.util.Arrays.asList(
                        SECKILL_TOP_BUYER_RECORDED_KEY + hashTag + ":" + voucherOrder.getId(),
                        SECKILL_TOP_BUYERS_DAILY_KEY + hashTag + ":" + day),
                voucherOrder.getUserId().toString(),
                String.valueOf(ttlMillis));
    }

    @Override
    public void rollbackCancelledOrder(VoucherOrder voucherOrder) {
        if (!properties.isEnabled() || !isValidOrder(voucherOrder)) {
            return;
        }
        Long shopId = resolveShopId(voucherOrder.getVoucherId());
        if (shopId == null) {
            return;
        }
        String day = resolveOrderTime(voucherOrder).toLocalDate().format(DAY_FORMATTER);
        String dailyKey = SECKILL_TOP_BUYERS_DAILY_KEY
                + "{" + shopId + "}:" + day;
        stringRedisTemplate.execute(
                ROLLBACK_SCRIPT,
                Collections.singletonList(dailyKey),
                voucherOrder.getUserId().toString());
    }

    @Override
    public List<TopBuyerDTO> queryTopBuyers(Long shopId, int days, int limit) {
        if (shopId == null || days <= 0 || limit <= 0) {
            return Collections.emptyList();
        }
        Map<Long, Double> scores = aggregateScores(shopId, days, limit);
        List<Map.Entry<Long, Double>> ranking = new ArrayList<>(scores.entrySet());
        ranking.sort(Map.Entry.<Long, Double>comparingByValue(Comparator.reverseOrder())
                .thenComparing(Map.Entry.comparingByKey()));
        List<TopBuyerDTO> result = new ArrayList<>();
        for (Map.Entry<Long, Double> entry : ranking) {
            if (result.size() >= limit) {
                break;
            }
            User user = userMapper.selectById(entry.getKey());
            UserInfo userInfo = userInfoMapper.selectById(entry.getKey());
            result.add(new TopBuyerDTO()
                    .setUserId(entry.getKey())
                    .setNickName(user == null ? null : user.getNickName())
                    .setIcon(user == null ? null : user.getIcon())
                    .setLevel(userInfo == null ? 0 : userInfo.getLevel())
                    .setScore(entry.getValue()));
        }
        return result;
    }

    @Override
    public Set<Long> queryTopBuyerUserIds(Long shopId, int days, int limit) {
        List<TopBuyerDTO> topBuyers = queryTopBuyers(shopId, days, limit);
        Set<Long> userIds = new LinkedHashSet<>();
        for (TopBuyerDTO topBuyer : topBuyers) {
            userIds.add(topBuyer.getUserId());
        }
        return userIds;
    }

    private Map<Long, Double> aggregateScores(Long shopId, int days, int limit) {
        Map<Long, Double> scores = new HashMap<>();
        int perDayLimit = Math.max(limit * 2, limit);
        for (int dayOffset = 0; dayOffset < days; dayOffset++) {
            String day = LocalDate.now().minusDays(dayOffset).format(DAY_FORMATTER);
            String key = SECKILL_TOP_BUYERS_DAILY_KEY + "{" + shopId + "}:" + day;
            Set<ZSetOperations.TypedTuple<String>> tuples = stringRedisTemplate.opsForZSet()
                    .reverseRangeWithScores(key, 0, perDayLimit - 1L);
            if (tuples == null) {
                continue;
            }
            for (ZSetOperations.TypedTuple<String> tuple : tuples) {
                if (tuple.getValue() == null || tuple.getScore() == null) {
                    continue;
                }
                try {
                    Long userId = Long.valueOf(tuple.getValue());
                    scores.merge(userId, tuple.getScore(), Double::sum);
                } catch (NumberFormatException ignored) {
                    // 跳过异常成员。
                }
            }
        }
        return scores;
    }

    private boolean isValidOrder(VoucherOrder voucherOrder) {
        return voucherOrder != null
                && voucherOrder.getId() != null
                && voucherOrder.getUserId() != null
                && voucherOrder.getVoucherId() != null;
    }

    private LocalDateTime resolveOrderTime(VoucherOrder voucherOrder) {
        return voucherOrder.getCreateTime() == null
                ? LocalDateTime.now() : voucherOrder.getCreateTime();
    }

    private Long resolveShopId(Long voucherId) {
        Object cached = stringRedisTemplate.opsForHash()
                .get(SECKILL_VOUCHER_META_KEY + voucherId, "shopId");
        if (cached != null) {
            try {
                return Long.valueOf(cached.toString());
            } catch (NumberFormatException ignored) {
                // Redis元数据异常时回源数据库。
            }
        }
        Voucher voucher = voucherMapper.selectById(voucherId);
        return voucher == null ? null : voucher.getShopId();
    }

    private static DefaultRedisScript<Long> loadScript(String resourceName) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(new ClassPathResource(resourceName)));
        script.setResultType(Long.class);
        return script;
    }
}
