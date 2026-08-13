package com.hmdp.service;

import com.hmdp.config.SeckillMarketingProperties;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.UserInfoMapper;
import com.hmdp.mapper.UserMapper;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.service.impl.SeckillTopBuyerServiceImpl;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.Arrays;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

class SeckillTopBuyerServiceImplTest {

    @Test
    @SuppressWarnings("unchecked")
    void successfulOrderIsRecordedOncePerShopAndDay() {
        SeckillTopBuyerServiceImpl service = new SeckillTopBuyerServiceImpl();
        StringRedisTemplate redisTemplate = Mockito.mock(StringRedisTemplate.class);
        HashOperations<String, Object, Object> hashOperations = Mockito.mock(HashOperations.class);
        Mockito.when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        Mockito.when(hashOperations.get("seckill:meta:2", "shopId")).thenReturn("8");
        ReflectionTestUtils.setField(service, "stringRedisTemplate", redisTemplate);
        ReflectionTestUtils.setField(service, "voucherMapper", Mockito.mock(VoucherMapper.class));
        ReflectionTestUtils.setField(service, "userMapper", Mockito.mock(UserMapper.class));
        ReflectionTestUtils.setField(service, "userInfoMapper", Mockito.mock(UserInfoMapper.class));
        ReflectionTestUtils.setField(service, "properties", new SeckillMarketingProperties());

        VoucherOrder order = new VoucherOrder()
                .setId(1001L)
                .setUserId(7L)
                .setVoucherId(2L)
                .setCreateTime(LocalDateTime.of(2026, 8, 10, 12, 0));
        service.recordSuccessfulOrder(order);

        Mockito.verify(redisTemplate).execute(
                any(RedisScript.class),
                eq(Arrays.asList(
                        "seckill:shop:top-buyers:recorded:{8}:1001",
                        "seckill:shop:top-buyers:daily:{8}:20260810")),
                eq("7"), eq("2592000000"));
    }
}
