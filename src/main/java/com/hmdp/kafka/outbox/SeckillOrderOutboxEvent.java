package com.hmdp.kafka.outbox;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;

@Data
@Accessors(chain = true)
@TableName("tb_seckill_order_outbox")
public class SeckillOrderOutboxEvent {

    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_SENT = "SENT";
    public static final String STATUS_COMPLETED = "COMPLETED";
    public static final String STATUS_MANUAL_REVIEW = "MANUAL_REVIEW";

    @TableId(value = "id", type = IdType.AUTO)
    private Long id;
    private String eventId;
    private Long orderId;
    private Long voucherId;
    private Long userId;
    private Boolean autoIssued;
    private String status;
    private Integer retryCount;
    private LocalDateTime nextRetryTime;
    private String lastError;
    private String relayOwner;
    private LocalDateTime relayLeaseUntil;
    private LocalDateTime createdTime;
    private LocalDateTime updatedTime;
    private LocalDateTime sentTime;
    private LocalDateTime completedTime;

    public static SeckillOrderOutboxEvent pending(
            Long orderId, Long voucherId, Long userId, boolean autoIssued) {
        LocalDateTime now = LocalDateTime.now();
        return new SeckillOrderOutboxEvent()
                .setEventId(String.valueOf(orderId))
                .setOrderId(orderId)
                .setVoucherId(voucherId)
                .setUserId(userId)
                .setAutoIssued(autoIssued)
                .setStatus(STATUS_PENDING)
                .setRetryCount(0)
                .setNextRetryTime(now)
                .setCreatedTime(now)
                .setUpdatedTime(now);
    }
}
