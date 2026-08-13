package com.hmdp.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "hmdp.seckill.access-token")
public class SeckillAccessTokenProperties {

    private boolean enabled = true;

    private long ttlSeconds = 30L;
}
