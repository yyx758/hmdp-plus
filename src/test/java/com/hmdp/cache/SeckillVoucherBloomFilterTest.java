package com.hmdp.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SeckillVoucherBloomFilterTest {

    private RBloomFilter<Long> redisBloomFilter;
    private SeckillVoucherBloomFilter bloomFilter;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        RedissonClient redissonClient = Mockito.mock(RedissonClient.class);
        redisBloomFilter = Mockito.mock(RBloomFilter.class);
        Mockito.when(redissonClient.<Long>getBloomFilter("bloom:seckill:voucher"))
                .thenReturn(redisBloomFilter);
        bloomFilter = new SeckillVoucherBloomFilter(redissonClient, 1000, 0.01D);
    }

    @Test
    void initializedFilterRejectsDefinitelyMissingIds() {
        Mockito.when(redisBloomFilter.contains(999L)).thenReturn(false);

        bloomFilter.initialize(Arrays.asList(2L, 3L));

        assertFalse(bloomFilter.mightContain(999L));
        Mockito.verify(redisBloomFilter).tryInit(1000L, 0.01D);
        Mockito.verify(redisBloomFilter).add(2L);
        Mockito.verify(redisBloomFilter).add(3L);
    }

    @Test
    void filterFailsOpenBeforeInitialization() {
        assertTrue(bloomFilter.mightContain(2L));
        Mockito.verify(redisBloomFilter, Mockito.never()).contains(Mockito.anyLong());
    }

    @Test
    void newlyCreatedVoucherIsAddedAtRuntime() {
        bloomFilter.put(8L);

        Mockito.verify(redisBloomFilter).tryInit(1000L, 0.01D);
        Mockito.verify(redisBloomFilter).add(8L);
    }
}
