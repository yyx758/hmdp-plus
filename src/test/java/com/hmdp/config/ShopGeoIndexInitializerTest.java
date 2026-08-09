package com.hmdp.config;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.data.redis.core.GeoOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ShopGeoIndexInitializerTest {

    private ShopGeoIndexInitializer initializer;
    private IShopService shopService;
    private StringRedisTemplate redisTemplate;
    private GeoOperations<String, String> geoOperations;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        initializer = Mockito.spy(new ShopGeoIndexInitializer());
        shopService = Mockito.mock(IShopService.class);
        redisTemplate = Mockito.mock(StringRedisTemplate.class);
        geoOperations = Mockito.mock(GeoOperations.class);

        Mockito.when(redisTemplate.opsForGeo()).thenReturn(geoOperations);
        Mockito.when(geoOperations.add(Mockito.anyString(), Mockito.anyCollection())).thenReturn(1L);
        Mockito.doReturn(Collections.emptySet()).when(initializer).scanGeoKeys();
        ReflectionTestUtils.setField(initializer, "shopService", shopService);
        ReflectionTestUtils.setField(initializer, "stringRedisTemplate", redisTemplate);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void rebuildsEachTypeThroughTemporaryKey() {
        Mockito.when(shopService.list()).thenReturn(Arrays.asList(
                new Shop().setId(1L).setTypeId(1L).setX(120.1).setY(30.1),
                new Shop().setId(2L).setTypeId(1L).setX(120.2).setY(30.2),
                new Shop().setId(3L).setTypeId(2L).setX(120.3).setY(30.3)
        ));

        initializer.rebuildGeoIndex();

        ArgumentCaptor<String> temporaryKeyCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(redisTemplate).rename(
                temporaryKeyCaptor.capture(),
                Mockito.eq("shop:geo:1"));
        assertTrue(temporaryKeyCaptor.getValue().startsWith("shop:geo:1:tmp:"));
        Mockito.verify(geoOperations).add(
                Mockito.startsWith("shop:geo:1:tmp:"),
                Mockito.argThat((Collection locations) -> locations.size() == 2));
        Mockito.verify(geoOperations).add(
                Mockito.startsWith("shop:geo:2:tmp:"),
                Mockito.argThat((Collection locations) -> locations.size() == 1));
    }
}
