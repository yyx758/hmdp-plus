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

    public static final String LOCK_SHOP_KEY = "lock:shop:";
    public static final Long LOCK_SHOP_TTL = 10L;
    public static final String LOCK_CACHE_CONSISTENCY_KEY = "lock:cache:consistency:";
    public static final String LOCK_SHOP_CACHE_OUTBOX_RELAY = "lock:shop:cache:outbox:relay";

    public static final String SECKILL_STOCK_KEY = "seckill:stock:";
    public static final String SECKILL_VOUCHER_META_KEY = "seckill:meta:";
    public static final String SECKILL_VOUCHER_NULL_KEY = "seckill:meta:null:";
    public static final String LOCK_SECKILL_VOUCHER_REBUILD_KEY = "lock:seckill:meta:rebuild:";
    public static final String SECKILL_VOUCHER_BLOOM_FILTER = "bloom:seckill:voucher";
    public static final String BLOG_LIKED_KEY = "blog:liked:";
    public static final String FEED_KEY = "feed:";
    public static final String SHOP_GEO_KEY = "shop:geo:";
    public static final String USER_SIGN_KEY = "sign:";
}
