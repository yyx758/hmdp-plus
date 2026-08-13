package com.hmdp.service;

import com.hmdp.config.SeckillAccessTokenProperties;
import com.hmdp.service.impl.SeckillAccessTokenServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;

class SeckillAccessTokenServiceImplTest {

    private SeckillAccessTokenServiceImpl service;
    private StringRedisTemplate redisTemplate;
    private SeckillAccessTokenProperties properties;

    @BeforeEach
    void setUp() {
        service = new SeckillAccessTokenServiceImpl();
        redisTemplate = Mockito.mock(StringRedisTemplate.class);
        properties = new SeckillAccessTokenProperties();
        properties.setEnabled(true);
        properties.setTtlSeconds(30L);
        ReflectionTestUtils.setField(service, "stringRedisTemplate", redisTemplate);
        ReflectionTestUtils.setField(service, "properties", properties);
    }

    @Test
    @SuppressWarnings("unchecked")
    void issueUsesAtomicRedisScriptAndReturnsStoredToken() {
        Mockito.when(redisTemplate.execute(
                any(RedisScript.class),
                eq(Collections.singletonList("seckill:access:token:{2}:7")),
                anyString(),
                eq("30000")))
                .thenReturn("existing-or-new-token");

        assertEquals("existing-or-new-token", service.issueAccessToken(2L, 7L));
    }

    @Test
    @SuppressWarnings("unchecked")
    void validateAndConsumeSucceedsOnlyWhenLuaDeletesToken() {
        Mockito.when(redisTemplate.execute(
                any(RedisScript.class),
                eq(Collections.singletonList("seckill:access:token:{2}:7")),
                eq("valid-token")))
                .thenReturn(1L);

        assertTrue(service.validateAndConsume(2L, 7L, "valid-token"));
        assertFalse(service.validateAndConsume(2L, 7L, ""));
    }

    @Test
    void disabledModeDoesNotTouchRedis() {
        properties.setEnabled(false);

        assertTrue(service.validateAndConsume(2L, 7L, "anything"));
        Mockito.verifyNoInteractions(redisTemplate);
    }
}
