package com.hmdp.config;

import cn.hutool.core.lang.UUID;
import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@Slf4j
@Component
public class ShopGeoIndexInitializer implements ApplicationRunner {

    @Resource
    private IShopService shopService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public void run(ApplicationArguments args) {
        try {
            rebuildGeoIndex();
        } catch (RuntimeException e) {
            log.error("初始化店铺GEO索引失败，应用将继续启动", e);
        }
    }

    void rebuildGeoIndex() {
        Map<Long, List<Shop>> shopsByType = shopService.list()
                .stream()
                .filter(this::hasGeoCoordinates)
                .collect(Collectors.groupingBy(Shop::getTypeId));

        Set<String> activeKeys = new HashSet<>();
        for (Map.Entry<Long, List<Shop>> entry : shopsByType.entrySet()) {
            String targetKey = SHOP_GEO_KEY + entry.getKey();
            String temporaryKey = targetKey + ":tmp:" + UUID.fastUUID().toString(true);
            List<RedisGeoCommands.GeoLocation<String>> locations =
                    new ArrayList<>(entry.getValue().size());
            for (Shop shop : entry.getValue()) {
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(), shop.getY())));
            }
            try {
                stringRedisTemplate.opsForGeo().add(temporaryKey, locations);
                stringRedisTemplate.rename(temporaryKey, targetKey);
                activeKeys.add(targetKey);
            } catch (RuntimeException e) {
                stringRedisTemplate.delete(temporaryKey);
                throw e;
            }
        }
        removeStaleGeoKeys(activeKeys);
        log.info("店铺GEO索引初始化完成，类型数={}", activeKeys.size());
    }

    private void removeStaleGeoKeys(Set<String> activeKeys) {
        Set<String> existingKeys = scanGeoKeys();
        List<String> staleKeys = existingKeys.stream()
                .filter(key -> !activeKeys.contains(key))
                .collect(Collectors.toList());
        if (!staleKeys.isEmpty()) {
            stringRedisTemplate.delete(staleKeys);
        }
    }

    Set<String> scanGeoKeys() {
        ScanOptions options = ScanOptions.scanOptions()
                .match(SHOP_GEO_KEY + "*")
                .count(100)
                .build();
        Set<String> keys = new HashSet<>();
        RedisConnection connection = stringRedisTemplate.getConnectionFactory().getConnection();
        try (Cursor<byte[]> cursor = connection.scan(options)) {
            while (cursor.hasNext()) {
                String key = new String(cursor.next(), StandardCharsets.UTF_8);
                String suffix = key.substring(SHOP_GEO_KEY.length());
                if (suffix.matches("\\d+")) {
                    keys.add(key);
                }
            }
        } finally {
            connection.close();
        }
        return keys;
    }

    private boolean hasGeoCoordinates(Shop shop) {
        return shop.getId() != null
                && shop.getTypeId() != null
                && shop.getX() != null
                && shop.getY() != null;
    }
}
