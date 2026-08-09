package com.hmdp.service;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.exception.OrderIdConflictException;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.impl.VoucherOrderPersistenceServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VoucherOrderPersistenceServiceTest {

    private VoucherOrderPersistenceServiceImpl persistenceService;
    private VoucherOrderMapper voucherOrderMapper;

    @BeforeEach
    void setUp() {
        persistenceService = new VoucherOrderPersistenceServiceImpl();
        voucherOrderMapper = Mockito.mock(VoucherOrderMapper.class);
        ReflectionTestUtils.setField(persistenceService, "voucherOrderMapper", voucherOrderMapper);
    }

    @Test
    void insertsBatchAndDecrementsHotStockRowOnlyOnce() {
        List<VoucherOrder> orders = Arrays.asList(order(1001L, 7L), order(1002L, 8L));
        Mockito.when(voucherOrderMapper.batchInsertIgnore(Mockito.anyList())).thenReturn(2);
        Mockito.when(voucherOrderMapper.decrementStock(2L, 2)).thenReturn(1);

        assertDoesNotThrow(() -> persistenceService.createVoucherOrders(orders));

        ArgumentCaptor<List<VoucherOrder>> captor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(voucherOrderMapper).batchInsertIgnore(captor.capture());
        assertEquals(2, captor.getValue().size());
        Mockito.verify(voucherOrderMapper).decrementStock(2L, 2);
    }

    @Test
    void doesNotDecrementStockWhenWholeBatchWasAlreadyPersisted() {
        VoucherOrder order = order(1001L, 7L);
        Mockito.when(voucherOrderMapper.batchInsertIgnore(Mockito.anyList())).thenReturn(0);
        Mockito.when(voucherOrderMapper.selectOrderId(7L, 2L)).thenReturn(1001L);

        assertDoesNotThrow(() -> persistenceService.createVoucherOrders(Collections.singletonList(order)));

        Mockito.verify(voucherOrderMapper, Mockito.never())
                .decrementStock(Mockito.anyLong(), Mockito.anyInt());
    }

    @Test
    void throwsWhenIgnoredOrderHasNoMatchingBusinessOrder() {
        Mockito.when(voucherOrderMapper.batchInsertIgnore(Mockito.anyList())).thenReturn(0);
        Mockito.when(voucherOrderMapper.selectOrderId(7L, 2L)).thenReturn(null);

        assertThrows(IllegalStateException.class,
                () -> persistenceService.createVoucherOrder(order(1001L, 7L)));
    }

    @Test
    void distinguishesSameMessageRedeliveryFromAnotherExistingOrder() {
        Mockito.when(voucherOrderMapper.batchInsertIgnore(Mockito.anyList())).thenReturn(0);
        Mockito.when(voucherOrderMapper.selectOrderId(7L, 2L)).thenReturn(9001L);

        OrderIdConflictException error = assertThrows(
                OrderIdConflictException.class,
                () -> persistenceService.createVoucherOrder(order(1001L, 7L))
        );

        assertEquals(1001L, error.getCurrentOrderId());
        assertEquals(9001L, error.getExistingOrderId());
    }

    @Test
    void throwsWhenAggregatedStockUpdateFails() {
        Mockito.when(voucherOrderMapper.batchInsertIgnore(Mockito.anyList())).thenReturn(1);
        Mockito.when(voucherOrderMapper.decrementStock(2L, 1)).thenReturn(0);

        assertThrows(IllegalStateException.class,
                () -> persistenceService.createVoucherOrder(order(1001L, 7L)));
    }

    @Test
    void rejectsIncompleteOrderBeforeWritingDatabase() {
        VoucherOrder incomplete = new VoucherOrder().setId(1001L).setUserId(7L);

        assertThrows(IllegalArgumentException.class,
                () -> persistenceService.createVoucherOrder(incomplete));
        Mockito.verifyNoInteractions(voucherOrderMapper);
    }

    private VoucherOrder order(long orderId, long userId) {
        return new VoucherOrder()
                .setId(orderId)
                .setUserId(userId)
                .setVoucherId(2L);
    }
}
