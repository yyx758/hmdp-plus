package com.hmdp.cache;

import com.hmdp.dto.SeckillVoucherCacheDTO;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class SeckillVoucherLocalCacheTest {

    @Test
    void storesReadsAndInvalidatesVoucherMetadata() {
        SeckillVoucherLocalCache cache = new SeckillVoucherLocalCache(true, 100, 30);
        SeckillVoucherCacheDTO voucher = new SeckillVoucherCacheDTO().setVoucherId(2L);

        cache.put(voucher);

        assertSame(voucher, cache.get(2L));
        cache.invalidate(2L);
        assertNull(cache.get(2L));
    }

    @Test
    void disabledCacheNeverStoresValues() {
        SeckillVoucherLocalCache cache = new SeckillVoucherLocalCache(false, 100, 30);

        cache.put(new SeckillVoucherCacheDTO().setVoucherId(2L));

        assertNull(cache.get(2L));
    }
}
