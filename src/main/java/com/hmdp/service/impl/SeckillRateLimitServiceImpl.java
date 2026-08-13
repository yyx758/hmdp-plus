package com.hmdp.service.impl;

import com.hmdp.config.SeckillRateLimitProperties;
import com.hmdp.dto.UserDTO;
import com.hmdp.enums.SeckillRateLimitScene;
import com.hmdp.exception.SeckillRateLimitException;
import com.hmdp.service.ISeckillRateLimitService;
import com.hmdp.service.SeckillOrderPressureService;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.Locale;

import static com.hmdp.utils.RedisConstants.SECKILL_RATE_LIMIT_IP_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_RATE_LIMIT_POLICY_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_RATE_LIMIT_USER_KEY;

@Service
public class SeckillRateLimitServiceImpl implements ISeckillRateLimitService {

    private static final long CODE_ALLOWED = 0L;
    private static final long CODE_IP_EXCEEDED = 10007L;
    private static final long CODE_USER_EXCEEDED = 10008L;

    private static final DefaultRedisScript<Long> TOKEN_BUCKET_SCRIPT = loadScript();

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private SeckillRateLimitProperties properties;

    @Resource
    private SeckillOrderPressureService pressureService;

    @Override
    public void check(
            Long voucherId,
            UserDTO user,
            String clientIp,
            SeckillRateLimitScene scene) {
        if (!properties.isEnabled()) {
            return;
        }
        if (voucherId == null || user == null || user.getId() == null || scene == null) {
            throw new IllegalArgumentException("秒杀限流参数不完整");
        }

        String normalizedIp = normalizeIp(clientIp);
        //如果ip白名单里或者用户白名单里已经有当前ip或者用户了,就不用操作了
        if (properties.getIpWhitelist().contains(normalizedIp)
                || properties.getUserWhitelist().contains(user.getId())) {
            return;
        }

        //得到场景
        SeckillRateLimitProperties.EndpointLimit limit = resolveLimit(scene);
        String hashTag = "{" + voucherId + "}";
        //获取场景key,不同场景限流配置不一样
        String sceneKey = scene.getKey();
        //ip限流
        String ipBucketKey = SECKILL_RATE_LIMIT_IP_KEY
                + hashTag + ":" + sceneKey + ":" + normalizedIp;
        //用户限流
        String userBucketKey = SECKILL_RATE_LIMIT_USER_KEY
                + hashTag + ":" + sceneKey + ":" + user.getId();
        //某一张秒杀券在某一个业务场景下
        String policyKey = SECKILL_RATE_LIMIT_POLICY_KEY + hashTag + ":" + sceneKey;

        Long result;
        try {
            result = stringRedisTemplate.execute(
                    TOKEN_BUCKET_SCRIPT,
                    //传入三个key
                    Arrays.asList(ipBucketKey, userBucketKey, policyKey),
                    //五个参数
                    String.valueOf(limit.getIpWindowMillis()),
                    String.valueOf(limit.getIpCapacity()),
                    String.valueOf(limit.getUserWindowMillis()),
                    String.valueOf(limit.getUserCapacity()),
                    String.valueOf(resolveCapacityMultiplier(user)),
                    String.valueOf(scene == SeckillRateLimitScene.SECKILL_ORDER
                            ? pressureService.getAdmissionMultiplier() : 1.0D));
        } catch (RuntimeException e) {
            throw new SeckillRateLimitException("限流服务暂时不可用，请稍后重试", e);
        }

        if (Long.valueOf(CODE_ALLOWED).equals(result)) {
            return;
        }
        if (Long.valueOf(CODE_IP_EXCEEDED).equals(result)) {
            throw new SeckillRateLimitException("当前网络请求过于频繁，请稍后重试");
        }
        if (Long.valueOf(CODE_USER_EXCEEDED).equals(result)) {
            throw new SeckillRateLimitException("操作过于频繁，请稍后重试");
        }
        throw new SeckillRateLimitException("限流服务暂时不可用，请稍后重试");
    }

    /*
    * 判断是哪个场景,传入当前的scene,如果和ISSUE_SECKILL_ACCESS_TOKEN一致就是前置的抢令牌场景
    * 否则就是实际下单的场景
    * */
    private SeckillRateLimitProperties.EndpointLimit resolveLimit(SeckillRateLimitScene scene) {
        return scene == SeckillRateLimitScene.ISSUE_SECKILL_ACCESS_TOKEN
                ? properties.getIssueSeckillAccessToken()
                : properties.getSeckillOrder();
    }

    /*
    * 用户流量倍率计算:
    * vip*2,高消费*3,不可叠加
    * */
    private double resolveCapacityMultiplier(UserDTO user) {
        //默认为1.00
        double multiplier = 1.0D;
        //如果用户等级不为空且大于等于1,就乘2(这几个参数都是配置里可以调节的)
        if (user.getLevel() != null && user.getLevel() >= properties.getVipMinLevel()) {
            multiplier = Math.max(multiplier, properties.getVipCapacityMultiplier());
        }
        //如果用户消费不为空且大于等于1000,视为高消费用户,就乘3
        if (user.getCredits() != null
                && user.getCredits() >= properties.getHighValueMinCredits()) {
            multiplier = Math.max(multiplier, properties.getHighValueCapacityMultiplier());
        }
        return Math.max(1.0D, multiplier);
    }

    /*
    * 传入clientIp,检验是否为无效ip
    * 如果有效就统一转成标准的ip(小写)
    * */
    private String normalizeIp(String clientIp) {
        if (clientIp == null || clientIp.trim().isEmpty()) {
            return "unknown";
        }
        return clientIp.trim().toLowerCase(Locale.ROOT);
    }

    private static DefaultRedisScript<Long> loadScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(
                new ClassPathResource("lua/seckill_token_bucket.lua")));
        script.setResultType(Long.class);
        return script;
    }
}
