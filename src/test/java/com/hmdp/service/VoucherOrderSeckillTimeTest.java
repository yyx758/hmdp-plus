package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillOrderStatusDTO;
import com.hmdp.dto.SeckillVoucherCacheDTO;
import com.hmdp.dto.UserDTO;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;

class VoucherOrderSeckillTimeTest {

    private VoucherOrderServiceImpl voucherOrderService;
    private StringRedisTemplate redisTemplate;
    private HashOperations<String, Object, Object> hashOperations;
    private SeckillVoucherCacheService cacheService;

    @BeforeEach
    void setUp() {
        voucherOrderService = new VoucherOrderServiceImpl();
        redisTemplate = Mockito.mock(StringRedisTemplate.class);
        hashOperations = Mockito.mock(HashOperations.class);
        cacheService = Mockito.mock(SeckillVoucherCacheService.class);
        RedisIdWorker redisIdWorker = Mockito.mock(RedisIdWorker.class);
        Mockito.when(redisIdWorker.nextId("order")).thenReturn(1001L);

        ReflectionTestUtils.setField(voucherOrderService, "stringRedisTemplate", redisTemplate);
        ReflectionTestUtils.setField(voucherOrderService, "redisIdWorker", redisIdWorker);
        ReflectionTestUtils.setField(voucherOrderService, "seckillVoucherCacheService", cacheService);
        Mockito.when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        Mockito.when(cacheService.queryById(Mockito.anyLong())).thenReturn(
                new SeckillVoucherCacheDTO()
                        .setVoucherId(2L)
                        .setStatus(1)
                        .setBeginTime(LocalDateTime.now().minusHours(1))
                        .setEndTime(LocalDateTime.now().plusHours(1)));

        UserDTO user = new UserDTO();
        user.setId(7L);
        UserHolder.saveUser(user);
    }

    @AfterEach
    void tearDown() {
        UserHolder.removeUser();
    }

    @Test
    void returnsNotStartedMessageWhenLuaReturnsFour() {
        mockLuaResult(4L);

        Result result = voucherOrderService.seckillVoucher(2L);

        assertFalse(result.getSuccess());
        assertEquals("秒杀尚未开始", result.getErrorMsg());
        verifyCurrentTimeWasPassedToLua();
    }

    @Test
    void returnsEndedMessageWhenLuaReturnsFive() {
        mockLuaResult(5L);

        Result result = voucherOrderService.seckillVoucher(2L);

        assertFalse(result.getSuccess());
        assertEquals("秒杀已经结束", result.getErrorMsg());
    }

    @Test
    void returnsUnavailableMessageWhenLuaReturnsSix() {
        mockLuaResult(6L);

        Result result = voucherOrderService.seckillVoucher(2L);

        assertFalse(result.getSuccess());
        assertEquals("秒杀券已下架或不可用", result.getErrorMsg());
    }

    @Test
    void returnsProcessingInsteadOfFinalSuccessWhenRequestWasAccepted() {
        mockLuaResult(0L);

        Result result = voucherOrderService.seckillVoucher(2L);

        assertTrue(result.getSuccess());
        SeckillOrderStatusDTO status = (SeckillOrderStatusDTO) result.getData();
        assertEquals(1001L, status.getOrderId());
        assertEquals("PROCESSING", status.getStatus());
    }

    @Test
    void returnsOwnedOrderStatusFromRedis() {
        Map<Object, Object> fields = new LinkedHashMap<>();
        fields.put("userId", "7");
        fields.put("status", "DUPLICATE_EXISTING");
        fields.put("existingOrderId", "9001");
        fields.put("message", "已恢复原订单");
        Mockito.when(hashOperations.entries("seckill:order:result:1001"))
                .thenReturn(fields);

        Result result = voucherOrderService.querySeckillOrderStatus(1001L);

        assertTrue(result.getSuccess());
        SeckillOrderStatusDTO status = (SeckillOrderStatusDTO) result.getData();
        assertEquals("DUPLICATE_EXISTING", status.getStatus());
        assertEquals(9001L, status.getExistingOrderId());
    }

    @Test
    void doesNotExposeAnotherUsersOrderStatus() {
        Map<Object, Object> fields = new LinkedHashMap<>();
        fields.put("userId", "8");
        fields.put("status", "SUCCESS");
        Mockito.when(hashOperations.entries("seckill:order:result:1001"))
                .thenReturn(fields);

        Result result = voucherOrderService.querySeckillOrderStatus(1001L);

        assertFalse(result.getSuccess());
    }

    @SuppressWarnings("unchecked")
    private void mockLuaResult(Long result) {
        Mockito.when(redisTemplate.execute(
                any(RedisScript.class),
                eq(Collections.emptyList()),
                anyString(),
                anyString(),
                anyString()
        )).thenReturn(result);
        Mockito.when(redisTemplate.execute(
                any(RedisScript.class),
                eq(Collections.emptyList()),
                anyString(),
                anyString(),
                anyString(),
                anyString(),
                anyString()
        )).thenReturn(result);
    }

    @SuppressWarnings("unchecked")
    private void verifyCurrentTimeWasPassedToLua() {
        Mockito.verify(redisTemplate).execute(
                any(RedisScript.class),
                eq(Collections.emptyList()),
                eq("2"),
                eq("7"),
                eq("1001"),
                anyString(),
                anyString()
        );
    }
}
