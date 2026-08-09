package com.hmdp.config;

import com.hmdp.cache.SeckillVoucherBloomFilter;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherService;
import com.hmdp.service.SeckillVoucherRedisSynchronizer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.Arrays;

class SeckillVoucherStockInitializerTest {

    private SeckillVoucherStockInitializer initializer;
    private ISeckillVoucherService seckillVoucherService;
    private IVoucherService voucherService;
    private SeckillVoucherRedisSynchronizer redisSynchronizer;
    private SeckillVoucherBloomFilter bloomFilter;

    @BeforeEach
    void setUp() {
        initializer = new SeckillVoucherStockInitializer();
        seckillVoucherService = Mockito.mock(ISeckillVoucherService.class);
        voucherService = Mockito.mock(IVoucherService.class);
        redisSynchronizer = Mockito.mock(SeckillVoucherRedisSynchronizer.class);
        bloomFilter = Mockito.mock(SeckillVoucherBloomFilter.class);

        ReflectionTestUtils.setField(initializer, "seckillVoucherService", seckillVoucherService);
        ReflectionTestUtils.setField(initializer, "voucherService", voucherService);
        ReflectionTestUtils.setField(initializer, "seckillVoucherRedisSynchronizer", redisSynchronizer);
        ReflectionTestUtils.setField(initializer, "seckillVoucherBloomFilter", bloomFilter);
    }

    @Test
    void restoresOnlyMissingRedisStockKeysAndRefreshesMetadata() {
        LocalDateTime beginTime = LocalDateTime.of(2026, 8, 4, 10, 0);
        LocalDateTime endTime = LocalDateTime.of(2026, 8, 4, 12, 0);
        SeckillVoucher first = new SeckillVoucher()
                .setVoucherId(2L).setStock(100).setBeginTime(beginTime).setEndTime(endTime);
        SeckillVoucher second = new SeckillVoucher()
                .setVoucherId(3L).setStock(50).setBeginTime(beginTime).setEndTime(endTime);
        Mockito.when(seckillVoucherService.list()).thenReturn(Arrays.asList(first, second));
        Voucher firstInfo = new Voucher().setId(2L).setShopId(10L).setStatus(1);
        Voucher secondInfo = new Voucher().setId(3L).setShopId(11L).setStatus(2);
        Mockito.when(voucherService.listByIds(Arrays.asList(2L, 3L)))
                .thenReturn(Arrays.asList(firstInfo, secondInfo));
        Mockito.when(redisSynchronizer.initializeVoucher(first, firstInfo)).thenReturn(true);
        Mockito.when(redisSynchronizer.initializeVoucher(second, secondInfo)).thenReturn(false);

        initializer.restoreMissingStockKeys();

        Mockito.verify(bloomFilter).initialize(Arrays.asList(2L, 3L));
        Mockito.verify(redisSynchronizer).initializeVoucher(first, firstInfo);
        Mockito.verify(redisSynchronizer).initializeVoucher(second, secondInfo);
    }
}
