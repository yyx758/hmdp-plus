package com.hmdp.utils;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long LOGIN_CODE_TTL = 2L;
    public static final String LOGIN_USER_KEY = "login:token:";
    public static final String LOGIN_USER_INDEX_KEY = "redis:login:";
    public static final Long LOGIN_USER_TTL = 36000L;

    public static final Long CACHE_NULL_TTL = 2L;

    public static final Long CACHE_SHOP_TTL = 30L;
    public static final String CACHE_SHOP_KEY = "cache:shop:";
    public static final String SHOP_BLOOM_FILTER = "bloom:shop";

    public static final String LOCK_SHOP_KEY = "lock:shop:";
    public static final Long LOCK_SHOP_TTL = 10L;
    public static final String LOCK_CACHE_CONSISTENCY_KEY = "lock:cache:consistency:";
    public static final String LOCK_SHOP_CACHE_OUTBOX_RELAY = "lock:shop:cache:outbox:relay";
    public static final String LOCK_SECKILL_VOUCHER_CACHE_OUTBOX_RELAY =
            "lock:seckill:voucher:l1:outbox:relay";

    public static final String SECKILL_STOCK_KEY = "seckill:stock:";
    public static final String SECKILL_VOUCHER_META_KEY = "seckill:meta:";
    public static final String SECKILL_VOUCHER_NULL_KEY = "seckill:meta:null:";
    public static final String LOCK_SECKILL_VOUCHER_REBUILD_KEY = "lock:seckill:meta:rebuild:";
    public static final String SECKILL_VOUCHER_BLOOM_FILTER = "bloom:seckill:voucher";
    public static final String SECKILL_ACCESS_TOKEN_KEY = "seckill:access:token:";
    public static final String SECKILL_RATE_LIMIT_IP_KEY = "seckill:rate:ip:";
    public static final String SECKILL_RATE_LIMIT_USER_KEY = "seckill:rate:user:";
    public static final String SECKILL_RATE_LIMIT_POLICY_KEY = "seckill:rate:policy:";
    public static final String SECKILL_ORDER_KEY = "seckill:order:";
    public static final String SECKILL_ORDER_HANDOFF_KEY = "seckill:order:handoff:";
    public static final String SECKILL_ORDER_ACCEPTED_KEY = "seckill:order:accepted";
    public static final String SECKILL_ORDER_HANDOFF_RELAY_LEASE_KEY =
            "seckill:order:handoff:relay:lease";
    public static final String SECKILL_ORDER_HANDOFF_QUARANTINE_KEY =
            "seckill:order:handoff:quarantine";
    public static final String SECKILL_RECOVERY_KEY = "seckill:recovery:";
    public static final String SECKILL_RECONCILIATION_LEADER_KEY =
            SECKILL_RECOVERY_KEY + "all";
    public static final String SECKILL_SUBSCRIBE_USER_KEY = "seckill:subscribe:users:";
    public static final String SECKILL_SUBSCRIBE_QUEUE_KEY = "seckill:subscribe:queue:";
    public static final String SECKILL_SUBSCRIBE_STATUS_KEY = "seckill:subscribe:status:";
    public static final String SECKILL_TOP_BUYERS_DAILY_KEY = "seckill:shop:top-buyers:daily:";
    public static final String SECKILL_TOP_BUYER_RECORDED_KEY = "seckill:shop:top-buyers:recorded:";
    public static final String SECKILL_REMINDER_QUEUE = "seckill:reminder:ready";
    public static final String USER_NOTIFICATION_KEY = "notification:user:";
    public static final String BLOG_LIKED_KEY = "blog:liked:";
    public static final String FEED_KEY = "feed:";
    public static final String SHOP_GEO_KEY = "shop:geo:";
    public static final String USER_SIGN_KEY = "sign:";
}
