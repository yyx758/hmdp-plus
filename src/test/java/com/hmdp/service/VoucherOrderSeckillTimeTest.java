package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillOrderStatusDTO;
import com.hmdp.dto.SeckillVoucherCacheDTO;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.kafka.outbox.SeckillOrderOutboxEvent;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;

class VoucherOrderSeckillTimeTest {

    private VoucherOrderServiceImpl service;
    private StringRedisTemplate redisTemplate;
    private ISeckillAccessTokenService accessTokenService;
    private SeckillOrderOutboxService outboxService;
    private SeckillOrderOutboxMapper outboxMapper;
    private VoucherOrderMapper voucherOrderMapper;
    private ValueOperations<String, String> valueOperations;
    private HashOperations<String, Object, Object> hashOperations;

    @BeforeEach
    void setUp() {
        service = new VoucherOrderServiceImpl();
        redisTemplate = Mockito.mock(StringRedisTemplate.class);
        valueOperations = Mockito.mock(ValueOperations.class);
        hashOperations = Mockito.mock(HashOperations.class);
        Mockito.when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        Mockito.when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        accessTokenService = Mockito.mock(ISeckillAccessTokenService.class);
        outboxService = Mockito.mock(SeckillOrderOutboxService.class);
        outboxMapper = Mockito.mock(SeckillOrderOutboxMapper.class);
        voucherOrderMapper = Mockito.mock(VoucherOrderMapper.class);
        SeckillVoucherCacheService cacheService = Mockito.mock(SeckillVoucherCacheService.class);
        OrderIdGenerator orderIdGenerator = Mockito.mock(OrderIdGenerator.class);

        Mockito.when(orderIdGenerator.nextId()).thenReturn(1001L);
        Mockito.when(accessTokenService.isEnabled()).thenReturn(true);
        Mockito.when(cacheService.queryById(2L)).thenReturn(new SeckillVoucherCacheDTO()
                .setVoucherId(2L)
                .setStatus(1)
                .setBeginTime(LocalDateTime.now().minusHours(1))
                .setEndTime(LocalDateTime.now().plusHours(1)));

        ReflectionTestUtils.setField(service, "stringRedisTemplate", redisTemplate);
        ReflectionTestUtils.setField(service, "orderIdGenerator", orderIdGenerator);
        ReflectionTestUtils.setField(service, "seckillVoucherCacheService", cacheService);
        ReflectionTestUtils.setField(service, "seckillAccessTokenService", accessTokenService);
        ReflectionTestUtils.setField(service, "outboxService", outboxService);
        ReflectionTestUtils.setField(service, "outboxMapper", outboxMapper);
        ReflectionTestUtils.setField(service, "baseMapper", voucherOrderMapper);
        ReflectionTestUtils.setField(service, "acceptanceMode", "redis-handoff");

        UserDTO user = new UserDTO();
        user.setId(7L);
        UserHolder.saveUser(user);
    }

    @AfterEach
    void tearDown() {
        UserHolder.removeUser();
    }

    @Test
    void passesTokenAndCurrentTimeIntoAtomicLuaScript() {
        mockLuaResult(4L);

        Result result = service.seckillVoucher(2L, "access-token");

        assertFalse(result.getSuccess());
        Mockito.verify(redisTemplate).execute(
                any(RedisScript.class), eq(Collections.emptyList()),
                eq("2"), eq("7"), eq("1001"), anyString(), eq("access-token"), eq("1"));
    }

    @Test
    void returnsUserFacingSuccessWithoutWaitingForOutboxAfterLuaSucceeds() {
        mockLuaResult(0L);

        Result result = service.seckillVoucher(2L, "access-token");

        assertTrue(result.getSuccess());
        SeckillOrderStatusDTO status = (SeckillOrderStatusDTO) result.getData();
        assertEquals(1001L, status.getOrderId());
        assertEquals("SUCCESS", status.getStatus());
        assertEquals("抢券成功", status.getMessage());
        Mockito.verifyNoInteractions(outboxService);
    }

    @Test
    void redisHandoffModeDoesNotCallOutboxOnRequestThread() {
        mockLuaResult(0L);
        Mockito.when(outboxService.accept(1001L, 2L, 7L, false))
                .thenThrow(new IllegalStateException("database unavailable"));

        Result result = service.seckillVoucher(2L, "access-token");

        assertTrue(result.getSuccess());
        assertEquals(1001L, ((SeckillOrderStatusDTO) result.getData()).getOrderId());
        Mockito.verifyNoInteractions(outboxService);
    }

    @Test
    void returnsUnconfirmedWhenRedisDisconnectsDuringLuaExecution() {
        Mockito.when(redisTemplate.execute(
                any(RedisScript.class), eq(Collections.emptyList()),
                anyString(), anyString(), anyString(), anyString(), anyString(), anyString()))
                .thenThrow(new RedisConnectionFailureException("redis down"));

        Result result = service.seckillVoucher(2L, "access-token");

        assertFalse(result.getSuccess());
        SeckillOrderStatusDTO status = (SeckillOrderStatusDTO) result.getData();
        assertEquals(1001L, status.getOrderId());
        assertEquals("PROCESSING", status.getStatus());
        Mockito.verifyNoInteractions(outboxService);
    }

    @Test
    void returnsCompletedOrderFromMysql() {
        Mockito.when(voucherOrderMapper.selectById(1001L)).thenReturn(new VoucherOrder()
                .setId(1001L).setUserId(7L).setVoucherId(2L).setStatus(1));

        Result result = service.querySeckillOrderStatus(1001L);

        assertTrue(result.getSuccess());
        assertEquals("SUCCESS", ((SeckillOrderStatusDTO) result.getData()).getStatus());
    }

    @Test
    void derivesProcessingStatusFromOutboxWithoutRedisStatusCopy() {
        Mockito.when(outboxMapper.findByOrderId(1001L)).thenReturn(
                SeckillOrderOutboxEvent.pending(1001L, 2L, 7L, false));

        Result result = service.querySeckillOrderStatus(1001L);

        assertTrue(result.getSuccess());
        assertEquals("PROCESSING", ((SeckillOrderStatusDTO) result.getData()).getStatus());
    }

    @Test
    void doesNotExposeAnotherUsersOutboxStatus() {
        Mockito.when(outboxMapper.findByOrderId(1001L)).thenReturn(
                SeckillOrderOutboxEvent.pending(1001L, 2L, 8L, false));

        assertFalse(service.querySeckillOrderStatus(1001L).getSuccess());
    }

    @Test
    void derivesInternalProcessingStatusFromRedisAcceptanceBeforeOutboxExists() {
        Mockito.when(hashOperations.get("seckill:order:accepted", "1001"))
                .thenReturn("7|2");

        Result result = service.querySeckillOrderStatus(1001L);

        assertTrue(result.getSuccess());
        assertEquals("PROCESSING", ((SeckillOrderStatusDTO) result.getData()).getStatus());
    }

    @Test
    void doesNotExposeAnotherUsersRedisAcceptance() {
        Mockito.when(hashOperations.get("seckill:order:accepted", "1001"))
                .thenReturn("8|2");

        assertFalse(service.querySeckillOrderStatus(1001L).getSuccess());
    }

    @SuppressWarnings("unchecked")
    private void mockLuaResult(Long result) {
        Mockito.when(redisTemplate.execute(
                any(RedisScript.class), eq(Collections.emptyList()),
                anyString(), anyString(), anyString(), anyString(), anyString(), anyString()))
                .thenReturn(result);
    }
}
