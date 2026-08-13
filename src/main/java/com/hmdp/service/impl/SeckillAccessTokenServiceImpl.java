package com.hmdp.service.impl;

import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.StrUtil;
import com.hmdp.config.SeckillAccessTokenProperties;
import com.hmdp.service.ISeckillAccessTokenService;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;

import static com.hmdp.utils.RedisConstants.SECKILL_ACCESS_TOKEN_KEY;

@Service
public class SeckillAccessTokenServiceImpl implements ISeckillAccessTokenService {

    private static final DefaultRedisScript<String> ISSUE_SCRIPT =
            loadScript("lua/seckill_access_token_issue.lua", String.class);

    private static final DefaultRedisScript<Long> CONSUME_SCRIPT =
            loadScript("lua/seckill_access_token_consume.lua", Long.class);

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private SeckillAccessTokenProperties properties;

    @Override
    public boolean isEnabled() {
        return properties.isEnabled();
    }

    /*
    * 核心就是执行lua脚本,获取access token,如果不存在就创建
    * */
    @Override
    public String issueAccessToken(Long voucherId, Long userId) {
        validateIdentity(voucherId, userId);
        //用UUID生成随机字符串
        String candidate = UUID.randomUUID().toString(true);
        if (!properties.isEnabled()) {
            //如果没有启动秒杀访问令牌机制,直接返回这个随机字符串
            return candidate;
        }
        //过期时间,最短是1s
        long ttlMillis = Math.max(1L, properties.getTtlSeconds()) * 1000L;
        /*
        * 执行seckill_access_token_issue.lua这个脚本
        * 传入的key,可以直接用集合表示,这里是只需要传入一个key,也就是singletonLis
        * 后续依次对应ARGV[1],ARGV[2]
        * */
        String token = stringRedisTemplate.execute(
                ISSUE_SCRIPT,
                Collections.singletonList(buildTokenKey(voucherId, userId)),
                candidate,
                String.valueOf(ttlMillis));
        //如果token还是为空
        if (StrUtil.isBlank(token)) {
            throw new IllegalStateException("秒杀资格令牌生成失败");
        }
        //否则返回token
        return token;
    }

    /*
    * 验证并消费
    * */
    @Override
    public boolean validateAndConsume(Long voucherId, Long userId, String token) {
        validateIdentity(voucherId, userId);
        //如果没启动前置令牌校验,直接返回true,代表可以直接进行后续消费
        if (!properties.isEnabled()) {
            return true;
        }
        //未传入token,不允许消费
        if (StrUtil.isBlank(token)) {
            return false;
        }
        /*
        * 执行seckill_access_token_consume.lua脚本进行消费
        * 传入参数是key和token
        * 核心就是核验传入的token和redis中实际的token是否一致,也就是令牌校验
        * */
        Long result = stringRedisTemplate.execute(
                CONSUME_SCRIPT,
                Collections.singletonList(buildTokenKey(voucherId, userId)),
                token);
        //1代表删除成功也就是true;0代表删除失败也就是false
        return Long.valueOf(1L).equals(result);
    }

    /*
    * 确认传入的两个参数,voucherId和userId不为空,为空直接抛出异常就好了
    * */
    private void validateIdentity(Long voucherId, Long userId) {
        if (voucherId == null || userId == null) {
            throw new IllegalArgumentException("秒杀券ID和用户ID不能为空");
        }
    }
   /*
   * 为了redis集群创建TokenKey,SECKILL_ACCESS_TOKEN_KEY + "{" + voucherId + "}:" + userId;
   * {voucherId}:用于让redis根据voucherId把相同卷的多个业务key分配到一个solt里面,否则会报错
   * "seckill:access:token:{vocherId}+userId"
   * */
    private String buildTokenKey(Long voucherId, Long userId) {
        return SECKILL_ACCESS_TOKEN_KEY + "{" + voucherId + "}:" + userId;
    }

    /*
    * 用于初始化加载好脚本,无需每次调用时加载
    * */
    private static <T> DefaultRedisScript<T> loadScript(
            String resourceName, Class<T> resultType) {
        DefaultRedisScript<T> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(new ClassPathResource(resourceName)));
        script.setResultType(resultType);
        return script;
    }
}
