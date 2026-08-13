package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

class VoucherOrderCancellationTest {

    @AfterEach
    void tearDown() {
        UserHolder.removeUser();
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void cancellationReturnsMysqlAndRedisStock() {
        VoucherOrderServiceImpl service = new VoucherOrderServiceImpl();
        VoucherOrderMapper orderMapper = Mockito.mock(VoucherOrderMapper.class);
        SeckillVoucherMapper voucherMapper = Mockito.mock(SeckillVoucherMapper.class);
        StringRedisTemplate redisTemplate = Mockito.mock(StringRedisTemplate.class);
        HashOperations<String, Object, Object> hashOperations = Mockito.mock(HashOperations.class);
        ISeckillSubscriptionService subscriptionService =
                Mockito.mock(ISeckillSubscriptionService.class);
        ISeckillTopBuyerService topBuyerService = Mockito.mock(ISeckillTopBuyerService.class);
        IUserNotificationService notificationService = Mockito.mock(IUserNotificationService.class);
        ReflectionTestUtils.setField(service, "baseMapper", orderMapper);
        ReflectionTestUtils.setField(service, "seckillVoucherMapper", voucherMapper);
        ReflectionTestUtils.setField(service, "stringRedisTemplate", redisTemplate);
        ReflectionTestUtils.setField(service, "seckillSubscriptionService", subscriptionService);
        ReflectionTestUtils.setField(service, "seckillTopBuyerService", topBuyerService);
        ReflectionTestUtils.setField(service, "userNotificationService", notificationService);
        Mockito.when(redisTemplate.opsForHash()).thenReturn(hashOperations);

        VoucherOrder order = new VoucherOrder()
                .setId(1001L).setUserId(7L).setVoucherId(2L)
                .setCreateTime(LocalDateTime.of(2026, 8, 10, 12, 0));
        Mockito.when(orderMapper.selectActiveOrder(7L, 2L)).thenReturn(order);
        Mockito.when(orderMapper.cancelActiveOrder(1001L, 7L)).thenReturn(1);
        Mockito.when(voucherMapper.adjustStock(2L, 1)).thenReturn(1);
        Mockito.when(redisTemplate.execute(
                any(RedisScript.class), Mockito.anyList(), eq("7"))).thenReturn(1L);

        UserDTO user = new UserDTO();
        user.setId(7L);
        UserHolder.saveUser(user);
        TransactionSynchronizationManager.initSynchronization();
        Result result = service.cancelVoucherOrder(2L);
        for (TransactionSynchronization synchronization
                : TransactionSynchronizationManager.getSynchronizations()) {
            synchronization.afterCompletion(TransactionSynchronization.STATUS_COMMITTED);
        }

        assertTrue(result.getSuccess());
        Mockito.verify(topBuyerService).rollbackCancelledOrder(order);
        Mockito.verify(subscriptionService).clearPurchased(order);
        Mockito.verify(notificationService).publish(
                eq(7L), eq("ORDER_CANCELLED"), eq("订单已取消"),
                Mockito.contains("库存已成功回流"), eq(2L), eq("cancel:1001"));
    }
}
