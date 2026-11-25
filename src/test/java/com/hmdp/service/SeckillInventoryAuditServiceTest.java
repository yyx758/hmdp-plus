package com.hmdp.service;

import com.hmdp.entity.SeckillVoucher;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

class SeckillInventoryAuditServiceTest {

    private ISeckillVoucherService voucherService;
    private SeckillOrderOutboxMapper outboxMapper;
    private VoucherOrderMapper orderMapper;
    private SeckillOrderHandoffService handoffService;
    private StringRedisTemplate redis;
    private ValueOperations<String, String> values;
    private SetOperations<String, String> sets;
    private ZSetOperations<String, String> sortedSets;
    private SeckillInventoryAuditService service;

    @BeforeEach
    void setUp() {
        voucherService = Mockito.mock(ISeckillVoucherService.class);
        outboxMapper = Mockito.mock(SeckillOrderOutboxMapper.class);
        orderMapper = Mockito.mock(VoucherOrderMapper.class);
        handoffService = Mockito.mock(SeckillOrderHandoffService.class);
        redis = Mockito.mock(StringRedisTemplate.class);
        values = Mockito.mock(ValueOperations.class);
        sets = Mockito.mock(SetOperations.class);
        sortedSets = Mockito.mock(ZSetOperations.class);
        Mockito.when(redis.opsForValue()).thenReturn(values);
        Mockito.when(redis.opsForSet()).thenReturn(sets);
        Mockito.when(redis.opsForZSet()).thenReturn(sortedSets);
        Mockito.when(handoffService.findAll(2L)).thenReturn(Collections.emptySet());
        service = new SeckillInventoryAuditService(
                voucherService, outboxMapper, orderMapper, handoffService, redis);
    }

    @Test
    void inFlightReservationsAreNotReconciledAgainstMysql() {
        Mockito.when(outboxMapper.countUnpersistedReservations(2L)).thenReturn(1);

        service.auditVoucher(2L);

        Mockito.verifyNoInteractions(voucherService, orderMapper, values, sets);
    }

    @Test
    void unresolvedAcceptanceReceiptsDeferAggregateComparison() {
        Mockito.when(sortedSets.zCard("seckill:order:accepted:audit")).thenReturn(1L);

        service.auditVoucher(2L);

        Mockito.verifyNoInteractions(voucherService, orderMapper, values, sets);
    }

    @Test
    void settledAuditOnlyReadsStockAndPurchaseSet() {
        Mockito.when(voucherService.getById(2L)).thenReturn(
                new SeckillVoucher().setVoucherId(2L).setStock(25));
        Mockito.when(values.get("seckill:stock:2")).thenReturn("20");
        Mockito.when(orderMapper.findActiveUserIds(2L)).thenReturn(Arrays.asList(7L, 8L));
        Mockito.when(sets.members("seckill:order:2"))
                .thenReturn(new HashSet<>(Collections.singletonList("7")));

        service.auditVoucher(2L);

        Mockito.verify(values).get("seckill:stock:2");
        Mockito.verify(sets).members("seckill:order:2");
        Mockito.verifyNoMoreInteractions(values, sets);
    }
}
