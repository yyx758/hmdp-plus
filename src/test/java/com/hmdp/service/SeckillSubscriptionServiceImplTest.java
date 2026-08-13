package com.hmdp.service;

import com.hmdp.config.SeckillMarketingProperties;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.service.impl.SeckillSubscriptionServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.TimeUnit;

class SeckillSubscriptionServiceImplTest {

    private SeckillSubscriptionServiceImpl service;
    private StringRedisTemplate redisTemplate;
    private SetOperations<String, String> setOperations;
    private ZSetOperations<String, String> zSetOperations;
    private HashOperations<String, Object, Object> hashOperations;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        service = new SeckillSubscriptionServiceImpl();
        redisTemplate = Mockito.mock(StringRedisTemplate.class);
        setOperations = Mockito.mock(SetOperations.class);
        zSetOperations = Mockito.mock(ZSetOperations.class);
        hashOperations = Mockito.mock(HashOperations.class);
        Mockito.when(redisTemplate.opsForSet()).thenReturn(setOperations);
        Mockito.when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        Mockito.when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        ReflectionTestUtils.setField(service, "stringRedisTemplate", redisTemplate);
        ReflectionTestUtils.setField(
                service, "seckillVoucherMapper", Mockito.mock(SeckillVoucherMapper.class));
        ReflectionTestUtils.setField(service, "properties", new SeckillMarketingProperties());
    }

    @Test
    void subscribeAddsUserToSubscriberSetAndQueue() {
        Mockito.when(setOperations.isMember(
                Mockito.anyString(), Mockito.anyString())).thenReturn(false);
        Mockito.when(setOperations.add(
                Mockito.anyString(), Mockito.anyString())).thenReturn(1L);
        Mockito.when(redisTemplate.getExpire(
                Mockito.anyString(), eq(TimeUnit.SECONDS))).thenReturn(3600L);

        service.subscribe(2L, 7L);

        Mockito.verify(setOperations).add(
                "seckill:subscribe:users:2", "7");
        Mockito.verify(zSetOperations).add(
                eq("seckill:subscribe:queue:2"), eq("7"), Mockito.anyDouble());
    }

    @Test
    void unsubscribeRemovesUserFromSubscriberSetAndQueue() {
        Mockito.when(setOperations.isMember(
                Mockito.anyString(), Mockito.anyString())).thenReturn(false);
        Mockito.when(hashOperations.get(
                Mockito.anyString(), Mockito.anyString())).thenReturn(null);

        service.unsubscribe(2L, 7L);

        Mockito.verify(setOperations).remove(
                "seckill:subscribe:users:2", "7");
        Mockito.verify(zSetOperations).remove(
                "seckill:subscribe:queue:2", "7");
    }

    private static <T> T eq(T value) {
        return Mockito.eq(value);
    }
}
