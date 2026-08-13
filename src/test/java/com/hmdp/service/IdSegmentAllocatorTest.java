package com.hmdp.service;

import com.hmdp.entity.IdSegment;
import com.hmdp.mapper.IdSegmentMapper;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IdSegmentAllocatorTest {

    @Test
    void locksAndAdvancesHighWaterMarkBeforeReturningRange() {
        IdSegmentMapper mapper = Mockito.mock(IdSegmentMapper.class);
        IdSegment row = new IdSegment();
        row.setBizTag("voucher-order");
        row.setMaxId(10000L);
        row.setStep(1000);
        Mockito.when(mapper.lockByBizTag("voucher-order")).thenReturn(row);
        Mockito.when(mapper.advance("voucher-order", 2000)).thenReturn(1);

        IdSegmentAllocator.Range range =
                new IdSegmentAllocator(mapper).allocate("voucher-order", 2000);

        assertEquals(10000L, range.getStartInclusive());
        assertEquals(12000L, range.getEndExclusive());
        InOrder order = Mockito.inOrder(mapper);
        order.verify(mapper).lockByBizTag("voucher-order");
        order.verify(mapper).advance("voucher-order", 2000);
    }

    @Test
    void refusesMissingConfiguration() {
        IdSegmentMapper mapper = Mockito.mock(IdSegmentMapper.class);
        assertThrows(IllegalStateException.class,
                () -> new IdSegmentAllocator(mapper).allocate("missing", 1000));
    }
}
