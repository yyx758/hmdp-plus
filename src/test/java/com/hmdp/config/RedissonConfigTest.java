package com.hmdp.config;

import org.junit.jupiter.api.Test;
import org.redisson.config.Config;
import org.redisson.config.SentinelServersConfig;
import org.redisson.config.SingleServerConfig;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;

import static org.assertj.core.api.Assertions.assertThat;

class RedissonConfigTest {

    @Test
    void createsSingleServerClientByDefault() {
        RedissonConfig config = new RedissonConfig();
        RedisProperties primaryProperties = new RedisProperties();
        primaryProperties.setHost("redis");
        primaryProperties.setPort(6379);
        primaryProperties.setPassword("123456");

        assertClientConfig(config.createConfig(primaryProperties),
                "redis://redis:6379", "123456");
    }

    @Test
    void createsSentinelClientWhenSentinelIsConfigured() {
        RedissonConfig config = new RedissonConfig();
        RedisProperties properties = new RedisProperties();
        properties.setPassword("123456");
        RedisProperties.Sentinel propertiesSentinel = new RedisProperties.Sentinel();
        propertiesSentinel.setMaster("hmdp-master");
        propertiesSentinel.setNodes(
                java.util.Arrays.asList("sentinel1:26379", "sentinel2:26379"));
        properties.setSentinel(propertiesSentinel);

        SentinelServersConfig sentinel = config.createConfig(properties).useSentinelServers();
        assertThat(sentinel.getMasterName()).isEqualTo("hmdp-master");
        assertThat(sentinel.getSentinelAddresses()).containsExactlyInAnyOrder(
                "redis://sentinel1:26379", "redis://sentinel2:26379");
        assertThat(sentinel.getPassword()).isEqualTo("123456");
    }

    private void assertClientConfig(Config config, String address, String password) {
        SingleServerConfig serverConfig = config.useSingleServer();
        assertThat(serverConfig.getAddress()).isEqualTo(address);
        assertThat(serverConfig.getPassword()).isEqualTo(password);
    }
}
