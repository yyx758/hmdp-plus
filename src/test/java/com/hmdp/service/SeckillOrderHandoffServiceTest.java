package com.hmdp.service;

import com.hmdp.kafka.outbox.SeckillOrderOutboxEvent;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.RedisScript;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

class SeckillOrderHandoffServiceTest {

    @Test
    void usesVoucherHashTagAndCompactRecoverableMember() {
        StringRedisTemplate redisTemplate = Mockito.mock(StringRedisTemplate.class);
        SeckillOrderHandoffService service = new SeckillOrderHandoffService(redisTemplate);

        assertEquals("seckill:order:handoff:{2}", service.buildKey(2L));
        assertEquals("1001|7|1", service.buildMember(1001L, 7L, true));

        SeckillOrderOutboxEvent event = service.parse(2L, "1001|7|1");
        assertEquals(1001L, event.getOrderId());
        assertEquals(7L, event.getUserId());
        assertEquals(2L, event.getVoucherId());
        assertEquals(true, event.getAutoIssued());
    }

    @Test
    void ignoresMalformedMembersInsteadOfCreatingInvalidOutbox() {
        SeckillOrderHandoffService service =
                new SeckillOrderHandoffService(Mockito.mock(StringRedisTemplate.class));

        assertNull(service.parse(2L, "1001|7"));
        assertNull(service.parse(2L, "bad|7|0"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void removesOnlyTheHandoffRepresentedByPersistedOutbox() {
        StringRedisTemplate redisTemplate = Mockito.mock(StringRedisTemplate.class);
        ZSetOperations<String, String> zSetOperations = Mockito.mock(ZSetOperations.class);
        Mockito.when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        SeckillOrderHandoffService service = new SeckillOrderHandoffService(redisTemplate);

        service.remove(SeckillOrderOutboxEvent.pending(1001L, 2L, 7L, false));

        Mockito.verify(zSetOperations).remove("seckill:order:handoff:{2}", "1001|7|0");
    }

    @Test
    @SuppressWarnings("unchecked")
    void readsAndRemovesHandoffsInBatches() {
        StringRedisTemplate redisTemplate = Mockito.mock(StringRedisTemplate.class);
        ZSetOperations<String, String> zSetOperations = Mockito.mock(ZSetOperations.class);
        Mockito.when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        SeckillOrderHandoffService service = new SeckillOrderHandoffService(redisTemplate);
        LinkedHashSet<String> members = new LinkedHashSet<>(
                Arrays.asList("1001|7|0", "1002|8|0"));
        Mockito.when(zSetOperations.range("seckill:order:handoff:{2}", 0, 99))
                .thenReturn(members);
        Mockito.when(zSetOperations.remove(
                "seckill:order:handoff:{2}", "1001|7|0", "1002|8|0"))
                .thenReturn(2L);

        assertEquals(members, service.findFirst(2L, 100));
        assertEquals(2L, service.removeBatch(2L, members));
    }

    @Test
    @SuppressWarnings("unchecked")
    void completesHandoffAndAcceptedMarkersInOneBatchScript() {
        StringRedisTemplate redisTemplate = Mockito.mock(StringRedisTemplate.class);
        SeckillOrderHandoffService service = new SeckillOrderHandoffService(redisTemplate);
        List<SeckillOrderOutboxEvent> events = Arrays.asList(
                SeckillOrderOutboxEvent.pending(1001L, 2L, 7L, false),
                SeckillOrderOutboxEvent.pending(1002L, 2L, 8L, false));
        LinkedHashSet<String> members = new LinkedHashSet<>(
                Arrays.asList("1001|7|0", "1002|8|0"));
        Mockito.when(redisTemplate.execute(
                Mockito.<RedisScript<Long>>any(), Mockito.anyList(), Mockito.<String[]>any()))
                .thenReturn(2L);

        assertEquals(2L, service.completeBatch(2L, events, members));

        Mockito.verify(redisTemplate).execute(
                Mockito.<RedisScript<Long>>any(),
                Mockito.eq(Arrays.asList(
                        "seckill:order:handoff:{2}", "seckill:order:accepted")),
                Mockito.eq("1001|7|0"), Mockito.eq("1001"),
                Mockito.eq("1002|8|0"), Mockito.eq("1002"));
    }
}
