package com.hmdp.service;

import com.hmdp.cache.SeckillVoucherBloomFilter;
import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillVoucherUpdateDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.service.impl.VoucherServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.LocalDateTime;

class VoucherServiceImplTest {

    private VoucherServiceImpl voucherService;
    private VoucherMapper voucherMapper;
    private ISeckillVoucherService seckillVoucherService;
    private SeckillVoucherRedisSynchronizer redisSynchronizer;
    private SeckillVoucherBloomFilter bloomFilter;

    @BeforeEach
    void setUp() {
        voucherService = new VoucherServiceImpl();
        voucherMapper = Mockito.mock(VoucherMapper.class);
        seckillVoucherService = Mockito.mock(ISeckillVoucherService.class);
        redisSynchronizer = Mockito.mock(SeckillVoucherRedisSynchronizer.class);
        bloomFilter = Mockito.mock(SeckillVoucherBloomFilter.class);
        ReflectionTestUtils.setField(voucherService, "baseMapper", voucherMapper);
        ReflectionTestUtils.setField(voucherService, "seckillVoucherService", seckillVoucherService);
        ReflectionTestUtils.setField(voucherService, "seckillVoucherRedisSynchronizer", redisSynchronizer);
        ReflectionTestUtils.setField(voucherService, "seckillVoucherBloomFilter", bloomFilter);
    }

    @Test
    void addSynchronizesStockAndMetadataWithoutWaitingForRestart() {
        LocalDateTime beginTime = LocalDateTime.of(2026, 8, 4, 10, 0);
        LocalDateTime endTime = LocalDateTime.of(2026, 8, 4, 12, 0);
        Voucher voucher = new Voucher()
                .setId(9L)
                .setStatus(1)
                .setStock(20)
                .setBeginTime(beginTime)
                .setEndTime(endTime);
        Mockito.when(voucherMapper.insert(voucher)).thenReturn(1);
        Mockito.when(seckillVoucherService.save(Mockito.any(SeckillVoucher.class))).thenReturn(true);

        voucherService.addSeckillVoucher(voucher);

        ArgumentCaptor<SeckillVoucher> captor = ArgumentCaptor.forClass(SeckillVoucher.class);
        Mockito.verify(redisSynchronizer).synchronizeNewVoucher(captor.capture(), Mockito.same(voucher));
        Mockito.verify(bloomFilter).put(9L);
        org.junit.jupiter.api.Assertions.assertEquals(9L, captor.getValue().getVoucherId());
        org.junit.jupiter.api.Assertions.assertEquals(20, captor.getValue().getStock());
    }

    @Test
    void addRollbackRemovesRedisProjection() {
        LocalDateTime beginTime = LocalDateTime.of(2026, 8, 4, 10, 0);
        LocalDateTime endTime = LocalDateTime.of(2026, 8, 4, 12, 0);
        Voucher voucher = new Voucher()
                .setId(9L).setStatus(1).setStock(20)
                .setBeginTime(beginTime).setEndTime(endTime);
        Mockito.when(voucherMapper.insert(voucher)).thenReturn(1);
        Mockito.when(seckillVoucherService.save(Mockito.any(SeckillVoucher.class))).thenReturn(true);
        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(true);
        try {
            voucherService.addSeckillVoucher(voucher);

            org.junit.jupiter.api.Assertions.assertEquals(
                    1, TransactionSynchronizationManager.getSynchronizations().size());
            TransactionSynchronizationManager.getSynchronizations().get(0)
                    .afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK);
            Mockito.verify(redisSynchronizer).deleteVoucher(9L);
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    void updateSynchronizesStatusAndTimeWithoutOverwritingStock() {
        LocalDateTime oldBegin = LocalDateTime.of(2026, 8, 4, 10, 0);
        LocalDateTime oldEnd = LocalDateTime.of(2026, 8, 4, 12, 0);
        LocalDateTime newEnd = LocalDateTime.of(2026, 8, 4, 13, 0);
        Mockito.when(voucherMapper.selectById(2L)).thenReturn(new Voucher().setId(2L).setStatus(1));
        Mockito.when(voucherMapper.updateById(Mockito.any(Voucher.class))).thenReturn(1);
        Mockito.when(seckillVoucherService.getById(2L)).thenReturn(new SeckillVoucher()
                .setVoucherId(2L).setStock(80).setBeginTime(oldBegin).setEndTime(oldEnd));
        Mockito.when(seckillVoucherService.updateById(Mockito.any(SeckillVoucher.class))).thenReturn(true);
        SeckillVoucherUpdateDTO update = new SeckillVoucherUpdateDTO();
        update.setVoucherId(2L);
        update.setStatus(2);
        update.setEndTime(newEnd);

        Result result = voucherService.updateSeckillVoucher(update);

        org.junit.jupiter.api.Assertions.assertTrue(result.getSuccess());
        ArgumentCaptor<SeckillVoucher> captor = ArgumentCaptor.forClass(SeckillVoucher.class);
        ArgumentCaptor<Voucher> voucherCaptor = ArgumentCaptor.forClass(Voucher.class);
        Mockito.verify(redisSynchronizer).synchronizeMetadata(
                captor.capture(), voucherCaptor.capture());
        org.junit.jupiter.api.Assertions.assertNull(captor.getValue().getStock());
        org.junit.jupiter.api.Assertions.assertEquals(newEnd, captor.getValue().getEndTime());
        org.junit.jupiter.api.Assertions.assertEquals(2L, voucherCaptor.getValue().getId());
        org.junit.jupiter.api.Assertions.assertEquals(2, voucherCaptor.getValue().getStatus());
        Mockito.verify(redisSynchronizer, Mockito.never())
                .synchronizeNewVoucher(
                        Mockito.any(SeckillVoucher.class), Mockito.any(Voucher.class));
    }
}
