package com.hmdp.service;

import com.hmdp.config.SeckillRateLimitProperties;
import com.hmdp.mapper.SeckillOrderOutboxMapper;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SeckillOrderPressureServiceTest {

    @Test
    void tightensAdmissionAsReliableBacklogGrows() {
        SeckillOrderOutboxMapper mapper = Mockito.mock(SeckillOrderOutboxMapper.class);
        SeckillRateLimitProperties properties = new SeckillRateLimitProperties();
        SeckillOrderPressureService service = new SeckillOrderPressureService(mapper, properties);

        Mockito.when(mapper.countBacklog()).thenReturn(6000L);
        service.refresh();

        assertEquals("CRITICAL", service.getLevel());
        assertEquals(0.1D, service.getAdmissionMultiplier());
        assertEquals(6000L, service.getBacklog());
    }
}
