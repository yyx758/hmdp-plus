package com.hmdp.kafka.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Idempotent cache invalidation command. The shop id is also used as the Kafka record key so
 * invalidations for one shop stay ordered in the same partition.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShopCacheInvalidationMessage {

    private String eventId;

    private Long shopId;

    private String reason;

    private long occurredAtEpochMillis;
}
