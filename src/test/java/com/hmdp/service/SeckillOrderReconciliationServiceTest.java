package com.hmdp.service;

import com.hmdp.entity.SeckillVoucher;
import com.hmdp.kafka.outbox.SeckillOrderOutboxEvent;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

class SeckillOrderReconciliationServiceTest {

    private ISeckillVoucherService voucherService;
    private SeckillOrderOutboxMapper outboxMapper;
    private VoucherOrderMapper orderMapper;
    private SeckillOrderHandoffService handoffService;
    private StringRedisTemplate redisTemplate;
    private SetOperations<String, String> setOperations;
    private SeckillOrderReconciliationService service;
    private AtomicReference<List<String>> rebuildArguments;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        voucherService = Mockito.mock(ISeckillVoucherService.class);
        outboxMapper = Mockito.mock(SeckillOrderOutboxMapper.class);
        orderMapper = Mockito.mock(VoucherOrderMapper.class);
        handoffService = Mockito.mock(SeckillOrderHandoffService.class);
        redisTemplate = Mockito.mock(StringRedisTemplate.class);
        ValueOperations<String, String> values = Mockito.mock(ValueOperations.class);
        setOperations = Mockito.mock(SetOperations.class);
        Mockito.when(redisTemplate.opsForValue()).thenReturn(values);
        Mockito.when(redisTemplate.opsForSet()).thenReturn(setOperations);
        Mockito.when(values.setIfAbsent(
                eq("seckill:recovery:2"), any(), eq(30L), eq(TimeUnit.SECONDS)))
                .thenReturn(true);
        rebuildArguments = new AtomicReference<>();
        Mockito.when(redisTemplate.execute(
                any(RedisScript.class), eq(Collections.emptyList()), Mockito.<String[]>any()))
                .thenAnswer(invocation -> {
                    Object[] args = invocation.getArguments();
                    List<String> captured = new java.util.ArrayList<>();
                    for (int index = 2; index < args.length; index++) {
                        Object value = args[index];
                        if (value instanceof String[]) {
                            captured.addAll(Arrays.asList((String[]) value));
                        } else {
                            captured.add(String.valueOf(value));
                        }
                    }
                    rebuildArguments.set(captured);
                    return 1L;
                });
        service = new SeckillOrderReconciliationService(
                voucherService, orderMapper, outboxMapper, handoffService, redisTemplate,
                30, 3600, 2);
    }

    @Test
    void rebuildsStockAndStagesPurchasedUsersInBoundedBatches() {
        Mockito.when(outboxMapper.findUnpersistedEvents(2L)).thenReturn(
                Collections.singletonList(
                        SeckillOrderOutboxEvent.pending(1001L, 2L, 7L, false)));
        Mockito.when(orderMapper.findActiveUserIds(2L)).thenReturn(Collections.singletonList(6L));
        Mockito.when(handoffService.findAll(2L)).thenReturn(
                new HashSet<>(Arrays.asList("1001|7|0", "1002|8|0")));
        Mockito.when(handoffService.parse(2L, "1001|7|0")).thenReturn(
                SeckillOrderOutboxEvent.pending(1001L, 2L, 7L, false));
        Mockito.when(handoffService.parse(2L, "1002|8|0")).thenReturn(
                SeckillOrderOutboxEvent.pending(1002L, 2L, 8L, false));

        service.reconcileVoucher(new SeckillVoucher().setVoucherId(2L).setStock(10));

        List<String> values = rebuildArguments.get();
        assertEquals("2", values.get(0));
        assertEquals("8", values.get(2));
        String stagingKey = values.get(3);
        Mockito.verify(setOperations).add(stagingKey, "__hmdp_rebuild_staging__");
        Mockito.verify(setOperations, Mockito.times(3)).add(
                eq(stagingKey), Mockito.<String[]>any());
    }

    @Test
    void scheduledFullReconciliationIncludesCurrentlyActiveVoucher() {
        java.time.LocalDateTime now = java.time.LocalDateTime.now();
        SeckillVoucher active = new SeckillVoucher().setVoucherId(2L).setStock(10)
                .setBeginTime(now.minusMinutes(1)).setEndTime(now.plusMinutes(1));
        Mockito.when(voucherService.list()).thenReturn(Collections.singletonList(active));
        Mockito.when(redisTemplate.opsForValue().setIfAbsent(
                eq("seckill:recovery:all"), any(), eq(3600L), eq(TimeUnit.SECONDS)))
                .thenReturn(true);

        service.reconcileAll();

        Mockito.verify(outboxMapper).findUnpersistedEvents(2L);
        Mockito.verify(orderMapper).findActiveUserIds(2L);
    }

    @Test
    void scheduledFullReconciliationSkipsScanWithoutGlobalLease() {
        Mockito.when(redisTemplate.opsForValue().setIfAbsent(
                eq("seckill:recovery:all"), any(), eq(3600L), eq(TimeUnit.SECONDS)))
                .thenReturn(false);

        service.reconcileAll();

        Mockito.verifyNoInteractions(voucherService);
    }
}
