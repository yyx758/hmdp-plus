package com.hmdp.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
public class SeckillVoucherCacheConfig {

    @Bean("seckillVoucherCacheRebuildExecutor")
    public Executor seckillVoucherCacheRebuildExecutor(
            @Value("${hmdp.cache.seckill-voucher.rebuild.core-pool-size:2}") int corePoolSize,
            @Value("${hmdp.cache.seckill-voucher.rebuild.max-pool-size:4}") int maxPoolSize,
            @Value("${hmdp.cache.seckill-voucher.rebuild.queue-capacity:200}") int queueCapacity) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(maxPoolSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setThreadNamePrefix("seckill-cache-rebuild-");
        // 队列满时拒绝本次刷新并继续返回旧值，不让请求线程同步查询数据库。
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        executor.initialize();
        return executor;
    }
}
