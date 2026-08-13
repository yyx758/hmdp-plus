package com.hmdp.service;

import com.hmdp.config.SeckillMarketingProperties;
import com.hmdp.service.impl.UserNotificationServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

class UserNotificationServiceImplTest {

    private UserNotificationServiceImpl service;
    private StringRedisTemplate redisTemplate;

    @BeforeEach
    void setUp() {
        service = new UserNotificationServiceImpl();
        redisTemplate = Mockito.mock(StringRedisTemplate.class);
        RedisIdWorker redisIdWorker = Mockito.mock(RedisIdWorker.class);
        Mockito.when(redisIdWorker.nextId("notification")).thenReturn(9001L);
        ReflectionTestUtils.setField(service, "stringRedisTemplate", redisTemplate);
        ReflectionTestUtils.setField(service, "redisIdWorker", redisIdWorker);
        ReflectionTestUtils.setField(service, "properties", new SeckillMarketingProperties());
    }

    @Test
    @SuppressWarnings("unchecked")
    void publishUsesUserHashTagAndDeduplicationKey() {
        Mockito.when(redisTemplate.execute(
                any(RedisScript.class),
                eq(Arrays.asList(
                        "notification:user:{7}:dedup:auto-issue:1001",
                        "notification:user:{7}:detail:9001",
                        "notification:user:{7}:inbox",
                        "notification:user:{7}:unread")),
                eq("9001"), eq("AUTO_ISSUE"), eq("到券提醒"),
                eq("已自动领取"), eq("2"), Mockito.anyString(),
                eq("300000"), eq("2592000000"), eq("100")))
                .thenReturn(1L);

        assertTrue(service.publish(
                7L, "AUTO_ISSUE", "到券提醒", "已自动领取",
                2L, "auto-issue:1001"));
    }
}
