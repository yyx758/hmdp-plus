package com.hmdp.enums;

public enum SeckillRateLimitScene {
     //申请秒杀 access token 的接口限流配置
    ISSUE_SECKILL_ACCESS_TOKEN("issue-seckill-access-token"),
    // 实际秒杀的接口限流配置
    SECKILL_ORDER("seckill-order");

    private final String key;

    SeckillRateLimitScene(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
