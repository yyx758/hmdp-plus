package com.hmdp.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@Data
@Component
@ConfigurationProperties(prefix = "hmdp.seckill.rate-limit")
public class SeckillRateLimitProperties {

    private boolean enabled = true;

    private boolean trustForwardedHeaders = false;

    private Set<String> ipWhitelist = new HashSet<>(Collections.emptySet());

    private Set<Long> userWhitelist = new HashSet<>(Collections.emptySet());

    private int vipMinLevel = 1;

    private int highValueMinCredits = 1000;

    private double vipCapacityMultiplier = 2.0D;

    private double highValueCapacityMultiplier = 3.0D;

    //抢令牌限流
    private EndpointLimit issueSeckillAccessToken = EndpointLimit.of(1000, 200, 1000, 2);

    //下单限流
    private EndpointLimit seckillOrder = EndpointLimit.of(1000, 200, 1000, 2);

    private Adaptive adaptive = new Adaptive();

    @Data
    public static class Adaptive {
        private boolean enabled = true;
        private long warningBacklog = 1000;
        private long criticalBacklog = 5000;
        private double warningMultiplier = 0.5D;
        private double criticalMultiplier = 0.1D;
    }

    /*
    * 限流配置类
    * */
    @Data
    public static class EndpointLimit {

        private int ipWindowMillis;

        private int ipCapacity;

        private int userWindowMillis;

        private int userCapacity;

        private static EndpointLimit of(
                int ipWindowMillis,
                int ipCapacity,
                int userWindowMillis,
                int userCapacity) {
            EndpointLimit limit = new EndpointLimit();
            limit.setIpWindowMillis(ipWindowMillis);
            limit.setIpCapacity(ipCapacity);
            limit.setUserWindowMillis(userWindowMillis);
            limit.setUserCapacity(userCapacity);
            return limit;
        }
    }
}
