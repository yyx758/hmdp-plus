package com.hmdp.kafka.outbox;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Accessors(chain = true)
@TableName("tb_shop_cache_invalidation_outbox")
public class ShopCacheInvalidationOutboxEvent {

    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_SENT = "SENT";
    public static final String STATUS_FAILED = "FAILED";

    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    private String eventId;

    private Long shopId;

    private String reason;

    private String status;

    private Integer retryCount;

    private LocalDateTime nextRetryTime;

    private String lastError;

    private LocalDateTime createdTime;

    private LocalDateTime updatedTime;

    private LocalDateTime sentTime;

    public static ShopCacheInvalidationOutboxEvent pending(Long shopId, String reason) {
        LocalDateTime now = LocalDateTime.now();
        return new ShopCacheInvalidationOutboxEvent()
                .setEventId(UUID.randomUUID().toString())
                .setShopId(shopId)
                .setReason(reason == null ? "shop-changed" : reason)
                .setStatus(STATUS_PENDING)
                .setRetryCount(0)
                .setNextRetryTime(now)
                .setCreatedTime(now)
                .setUpdatedTime(now);
    }
}
