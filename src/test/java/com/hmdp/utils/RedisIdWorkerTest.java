package com.hmdp.utils;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RedisIdWorkerTest {

    private static final long BEGIN_TIMESTAMP = 1640995200L;
    private static final ZoneId CHINA_ZONE = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd");

    @Test
    @SuppressWarnings("unchecked")
    void usesEpochSecondsAndChinaDateForRedisCounter() {
        StringRedisTemplate redisTemplate = Mockito.mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOperations = Mockito.mock(ValueOperations.class);
        Mockito.when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        Mockito.when(valueOperations.increment(Mockito.anyString())).thenReturn(7L);

        ZonedDateTime chinaTimeBefore = ZonedDateTime.now(CHINA_ZONE);
        long epochBefore = Instant.now().getEpochSecond();
        long id = new RedisIdWorker(redisTemplate).nextId("order");
        long epochAfter = Instant.now().getEpochSecond();
        ZonedDateTime chinaTimeAfter = ZonedDateTime.now(CHINA_ZONE);

        long timestamp = id >>> 32;
        long sequence = id & 0xFFFFFFFFL;
        long generatedEpoch = timestamp + BEGIN_TIMESTAMP;

        assertTrue(generatedEpoch >= epochBefore && generatedEpoch <= epochAfter);
        assertEquals(7L, sequence);

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(valueOperations).increment(keyCaptor.capture());
        String counterKey = keyCaptor.getValue();
        String expectedBefore = "icr:order:" + chinaTimeBefore.format(DATE_FORMATTER);
        String expectedAfter = "icr:order:" + chinaTimeAfter.format(DATE_FORMATTER);
        assertTrue(counterKey.equals(expectedBefore) || counterKey.equals(expectedAfter));
    }
}
