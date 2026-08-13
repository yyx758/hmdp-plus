package com.hmdp.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "hmdp.seckill.marketing")
public class SeckillMarketingProperties {

    private boolean enabled = true;

    private int subscriptionGraceSeconds = 86400;

    private int topBuyerRetentionDays = 30;

    private Reminder reminder = new Reminder();

    private Notification notification = new Notification();

    @Data
    public static class Reminder {

        private boolean enabled = true;

        private int leadSeconds = 120;

        private int maxAudience = 500;

        private int topBuyerDays = 30;

        private int topBuyerCount = 200;

        private int vipMinLevel = 1;

        private int vipCount = 200;
    }

    @Data
    public static class Notification {

        private int retentionDays = 30;

        private int maxItems = 100;

        private int dedupSeconds = 300;
    }
}
