package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.util.StringUtils;

@Configuration
public class RedissonConfig {

    @Bean(destroyMethod = "shutdown")
    @Primary
    public RedissonClient redissonClient(RedisProperties redisProperties) {
        return createClient(redisProperties.getHost(), redisProperties.getPort(), redisProperties.getPassword());
    }

    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient2(
            @Value("${hmdp.redisson.client2.host}") String host,
            @Value("${hmdp.redisson.client2.port}") int port,
            @Value("${hmdp.redisson.client2.password}") String password) {
        return createClient(host, port, password);
    }

    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient3(
            @Value("${hmdp.redisson.client3.host}") String host,
            @Value("${hmdp.redisson.client3.port}") int port,
            @Value("${hmdp.redisson.client3.password}") String password) {
        return createClient(host, port, password);
    }

    private RedissonClient createClient(String host, int port, String password) {
        Config config = new Config();
        SingleServerConfig serverConfig = config.useSingleServer()
                .setAddress("redis://" + host + ":" + port);
        if (StringUtils.hasText(password)) {
            serverConfig.setPassword(password);
        }
        return Redisson.create(config);
    }
}
