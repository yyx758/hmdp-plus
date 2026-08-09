package com.hmdp.utils;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;

/**
 * 只负责读取秒杀订单 Stream，并拥有独立的 Lettuce 连接工厂。
 */
public class SeckillStreamReader implements DisposableBean {

    private final LettuceConnectionFactory connectionFactory;
    private final StringRedisTemplate redisTemplate;

    public SeckillStreamReader(LettuceConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        this.redisTemplate = new StringRedisTemplate(connectionFactory);
        this.redisTemplate.afterPropertiesSet();
    }

    public List<MapRecord<String, Object, Object>> read(
            Consumer consumer,
            StreamReadOptions readOptions,
            StreamOffset<String> streamOffset) {
        return redisTemplate.opsForStream().read(consumer, readOptions, streamOffset);
    }

    @Override
    public void destroy() {
        connectionFactory.destroy();
    }
}
