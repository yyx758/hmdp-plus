package com.hmdp.config;

import com.hmdp.utils.SeckillStreamReader;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.util.StringUtils;

import java.time.Duration;

/**
 * 秒杀订单 Stream 读取连接配置。
 *
 * <p>阻塞式 XREADGROUP 会在 BLOCK 期间独占连接，因此不能与接口请求共用主连接池。</p>
 */
@Configuration
public class SeckillStreamRedisConfig {

    private static final String STREAM_READER_CLIENT_NAME = "hmdp-seckill-stream-reader";

    @Bean
    public SeckillStreamReader seckillStreamReader(
            RedisProperties redisProperties,
            @Value("${hmdp.seckill.consumer.worker-count:4}") int workerCount,
            @Value("${hmdp.seckill.consumer.stream-read-pool-max-active:12}") int maxActive) {
        if (workerCount <= 0) {
            throw new IllegalArgumentException("秒杀消费者线程数必须大于0");
        }
        if (maxActive < workerCount) {
            throw new IllegalArgumentException("Stream读取连接池最大连接数不能小于消费者线程数");
        }

        RedisStandaloneConfiguration serverConfig = new RedisStandaloneConfiguration();
        serverConfig.setHostName(redisProperties.getHost());
        serverConfig.setPort(redisProperties.getPort());
        serverConfig.setDatabase(redisProperties.getDatabase());
        if (StringUtils.hasText(redisProperties.getPassword())) {
            serverConfig.setPassword(RedisPassword.of(redisProperties.getPassword()));
        }

        GenericObjectPoolConfig<?> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(maxActive);
        poolConfig.setMaxIdle(maxActive);
        poolConfig.setMinIdle(workerCount);
        poolConfig.setMaxWaitMillis(3000L);

        LettucePoolingClientConfiguration clientConfig = LettucePoolingClientConfiguration.builder()
                .clientName(STREAM_READER_CLIENT_NAME)
                // XREADGROUP 最长阻塞2秒，命令超时必须大于阻塞时间。
                .commandTimeout(Duration.ofSeconds(5))
                .shutdownTimeout(Duration.ofMillis(100))
                .poolConfig(poolConfig)
                .build();
        LettuceConnectionFactory connectionFactory =
                new LettuceConnectionFactory(serverConfig, clientConfig);
        connectionFactory.afterPropertiesSet();
        return new SeckillStreamReader(connectionFactory);
    }
}
