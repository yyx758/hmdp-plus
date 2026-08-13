package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SentinelServersConfig;
import org.redisson.config.SingleServerConfig;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.util.StringUtils;

import java.util.List;

@Configuration
public class RedissonConfig {

    @Bean(destroyMethod = "shutdown")
    @Primary
    public RedissonClient redissonClient(RedisProperties redisProperties) {
        return Redisson.create(createConfig(redisProperties));
    }

    Config createConfig(RedisProperties redisProperties) {
        RedisProperties.Sentinel sentinel = redisProperties.getSentinel();
        if (sentinel != null && StringUtils.hasText(sentinel.getMaster())
                && sentinel.getNodes() != null && !sentinel.getNodes().isEmpty()) {
            return createSentinelConfig(sentinel, redisProperties.getPassword());
        }
        return createSingleServerConfig(
                redisProperties.getHost(), redisProperties.getPort(), redisProperties.getPassword());
    }

    private Config createSingleServerConfig(String host, int port, String password) {
        Config config = new Config();
        SingleServerConfig serverConfig = config.useSingleServer()
                .setAddress("redis://" + host + ":" + port);
        if (StringUtils.hasText(password)) {
            serverConfig.setPassword(password);
        }
        return config;
    }

    private Config createSentinelConfig(
            RedisProperties.Sentinel sentinel, String redisPassword) {
        Config config = new Config();
        SentinelServersConfig serverConfig = config.useSentinelServers()
                .setMasterName(sentinel.getMaster());
        List<String> nodes = sentinel.getNodes();
        for (String node : nodes) {
            serverConfig.addSentinelAddress("redis://" + node);
        }
        if (StringUtils.hasText(redisPassword)) {
            serverConfig.setPassword(redisPassword);
        }
        return config;
    }
}
