package com.hmdp.service;

import com.hmdp.config.SeckillRateLimitProperties;
import com.hmdp.dto.UserDTO;
import com.hmdp.enums.SeckillRateLimitScene;
import com.hmdp.exception.SeckillRateLimitException;
import com.hmdp.service.impl.SeckillRateLimitServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

class SeckillRateLimitServiceImplTest {

    private SeckillRateLimitServiceImpl service;
    private StringRedisTemplate redisTemplate;
    private SeckillRateLimitProperties properties;
    private UserDTO user;
    private SeckillOrderPressureService pressureService;

    @BeforeEach
    void setUp() {
        service = new SeckillRateLimitServiceImpl();
        redisTemplate = Mockito.mock(StringRedisTemplate.class);
        properties = new SeckillRateLimitProperties();
        user = new UserDTO();
        user.setId(7L);
        user.setLevel(0);
        user.setCredits(0);
        pressureService = Mockito.mock(SeckillOrderPressureService.class);
        Mockito.when(pressureService.getAdmissionMultiplier()).thenReturn(1.0D);
        ReflectionTestUtils.setField(service, "stringRedisTemplate", redisTemplate);
        ReflectionTestUtils.setField(service, "properties", properties);
        ReflectionTestUtils.setField(service, "pressureService", pressureService);
    }

    @Test
    @SuppressWarnings("unchecked")
    void allowedRequestUsesActivityScopedIpUserAndPolicyKeys() {
        Mockito.when(redisTemplate.execute(
                any(RedisScript.class),
                eq(Arrays.asList(
                        "seckill:rate:ip:{2}:issue-seckill-access-token:10.0.0.8",
                        "seckill:rate:user:{2}:issue-seckill-access-token:7",
                        "seckill:rate:policy:{2}:issue-seckill-access-token")),
                eq("1000"), eq("200"), eq("1000"), eq("2"), eq("1.0"), eq("1.0")))
                .thenReturn(0L);

        assertDoesNotThrow(() -> service.check(
                2L, user, "10.0.0.8",
                SeckillRateLimitScene.ISSUE_SECKILL_ACCESS_TOKEN));
    }

    @Test
    @SuppressWarnings("unchecked")
    void highValueUserGetsConfiguredCapacityMultiplier() {
        user.setLevel(2);
        user.setCredits(1500);
        Mockito.when(redisTemplate.execute(
                any(RedisScript.class),
                eq(Arrays.asList(
                        "seckill:rate:ip:{2}:seckill-order:10.0.0.8",
                        "seckill:rate:user:{2}:seckill-order:7",
                        "seckill:rate:policy:{2}:seckill-order")),
                eq("1000"), eq("200"), eq("1000"), eq("2"), eq("3.0"), eq("1.0")))
                .thenReturn(0L);

        assertDoesNotThrow(() -> service.check(
                2L, user, "10.0.0.8", SeckillRateLimitScene.SECKILL_ORDER));
    }

    @Test
    @SuppressWarnings("unchecked")
    void userBucketRejectionReturnsSpecificMessage() {
        Mockito.when(redisTemplate.execute(
                any(RedisScript.class),
                Mockito.anyList(),
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any()))
                .thenReturn(10008L);

        SeckillRateLimitException exception = assertThrows(
                SeckillRateLimitException.class,
                () -> service.check(
                        2L, user, "10.0.0.8", SeckillRateLimitScene.SECKILL_ORDER));

        assertEquals("操作过于频繁，请稍后重试", exception.getMessage());
    }

    @Test
    void userWhitelistBypassesRedis() {
        properties.getUserWhitelist().add(7L);

        assertDoesNotThrow(() -> service.check(
                2L, user, "10.0.0.8", SeckillRateLimitScene.SECKILL_ORDER));
        Mockito.verifyNoInteractions(redisTemplate);
    }
}
