package com.hmdp.config;

import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.redisson.config.SingleServerConfig;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;

import static org.assertj.core.api.Assertions.assertThat;

class RedissonConfigTest {

    @Test
    void createsThreeIndependentClients() {
        RedissonConfig config = new RedissonConfig();
        RedisProperties primaryProperties = new RedisProperties();
        primaryProperties.setHost("redis");
        primaryProperties.setPort(6379);
        primaryProperties.setPassword("123456");

        RedissonClient primary = config.redissonClient(primaryProperties);
        RedissonClient second = config.redissonClient2("redis2", 6379, "123456");
        RedissonClient third = config.redissonClient3("redis3", 6379, "123456");
        try {
            assertClientConfig(primary, "redis://redis:6379", "123456");
            assertClientConfig(second, "redis://redis2:6379", "123456");
            assertClientConfig(third, "redis://redis3:6379", "123456");
        } finally {
            primary.shutdown();
            second.shutdown();
            third.shutdown();
        }
    }

    private void assertClientConfig(RedissonClient client, String address, String password) {
        SingleServerConfig serverConfig = client.getConfig().useSingleServer();
        assertThat(serverConfig.getAddress()).isEqualTo(address);
        assertThat(serverConfig.getPassword()).isEqualTo(password);
    }
}
