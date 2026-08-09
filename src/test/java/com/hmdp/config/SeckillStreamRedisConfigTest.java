package com.hmdp.config;

import com.hmdp.utils.SeckillStreamReader;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SeckillStreamRedisConfigTest {

    @Test
    void createsDedicatedPoolForAllStreamWorkers() throws Exception {
        RedisProperties redisProperties = new RedisProperties();
        redisProperties.setHost("127.0.0.1");
        redisProperties.setPort(6380);
        redisProperties.setDatabase(2);
        redisProperties.setPassword("secret");

        SeckillStreamReader reader = new SeckillStreamRedisConfig()
                .seckillStreamReader(redisProperties, 8, 12);
        try {
            LettuceConnectionFactory factory = (LettuceConnectionFactory)
                    ReflectionTestUtils.getField(reader, "connectionFactory");
            assertThat(factory).isNotNull();
            assertThat(factory.getHostName()).isEqualTo("127.0.0.1");
            assertThat(factory.getPort()).isEqualTo(6380);
            assertThat(factory.getDatabase()).isEqualTo(2);
            assertThat(factory.getPassword()).isEqualTo("secret");

            LettucePoolingClientConfiguration clientConfiguration =
                    (LettucePoolingClientConfiguration) factory.getClientConfiguration();
            GenericObjectPoolConfig<?> poolConfig = clientConfiguration.getPoolConfig();
            assertThat(poolConfig.getMaxTotal()).isEqualTo(12);
            assertThat(poolConfig.getMaxIdle()).isEqualTo(12);
            assertThat(poolConfig.getMinIdle()).isEqualTo(8);
            assertThat(clientConfiguration.getCommandTimeout()).isEqualTo(Duration.ofSeconds(5));
            assertThat(clientConfiguration.getClientName())
                    .contains("hmdp-seckill-stream-reader");
        } finally {
            reader.destroy();
        }
    }

    @Test
    void rejectsPoolSmallerThanWorkerCount() {
        RedisProperties redisProperties = new RedisProperties();

        assertThatThrownBy(() -> new SeckillStreamRedisConfig()
                .seckillStreamReader(redisProperties, 8, 7))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("不能小于消费者线程数");
    }
}
