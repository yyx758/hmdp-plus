package com.hmdp.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hmdp.kafka.outbox.SeckillOrderOutboxEvent;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;
import java.util.List;

public interface SeckillOrderOutboxMapper extends BaseMapper<SeckillOrderOutboxEvent> {

    @Insert("INSERT IGNORE INTO tb_seckill_order_outbox "
            + "(event_id, order_id, voucher_id, user_id, auto_issued, status, retry_count, "
            + "next_retry_time, created_time, updated_time) VALUES "
            + "(#{event.eventId}, #{event.orderId}, #{event.voucherId}, #{event.userId}, "
            + "#{event.autoIssued}, #{event.status}, 0, #{event.nextRetryTime}, "
            + "#{event.createdTime}, #{event.updatedTime})")
    int insertIgnore(@Param("event") SeckillOrderOutboxEvent event);

    @Insert({"<script>",
            "INSERT IGNORE INTO tb_seckill_order_outbox ",
            "(event_id, order_id, voucher_id, user_id, auto_issued, status, retry_count, ",
            "next_retry_time, created_time, updated_time) VALUES ",
            "<foreach collection='events' item='event' separator=','>",
            "(#{event.eventId}, #{event.orderId}, #{event.voucherId}, #{event.userId}, ",
            "#{event.autoIssued}, #{event.status}, 0, #{event.nextRetryTime}, ",
            "#{event.createdTime}, #{event.updatedTime})",
            "</foreach>",
            "</script>"})
    int insertIgnoreBatch(@Param("events") List<SeckillOrderOutboxEvent> events);

    @Select({"<script>",
            "SELECT id, event_id AS eventId, order_id AS orderId, voucher_id AS voucherId, ",
            "user_id AS userId, auto_issued AS autoIssued, status, retry_count AS retryCount, ",
            "next_retry_time AS nextRetryTime, last_error AS lastError, ",
            "relay_owner AS relayOwner, relay_lease_until AS relayLeaseUntil, ",
            "created_time AS createdTime, updated_time AS updatedTime, sent_time AS sentTime, ",
            "completed_time AS completedTime FROM tb_seckill_order_outbox WHERE order_id IN ",
            "<foreach collection='orderIds' item='orderId' open='(' separator=',' close=')'>",
            "#{orderId}</foreach>",
            "</script>"})
    List<SeckillOrderOutboxEvent> findByOrderIds(@Param("orderIds") List<Long> orderIds);

    @Select("SELECT id, event_id AS eventId, order_id AS orderId, voucher_id AS voucherId, "
            + "user_id AS userId, auto_issued AS autoIssued, status, retry_count AS retryCount, "
            + "next_retry_time AS nextRetryTime, last_error AS lastError, "
            + "relay_owner AS relayOwner, relay_lease_until AS relayLeaseUntil, "
            + "created_time AS createdTime, updated_time AS updatedTime, sent_time AS sentTime, "
            + "completed_time AS completedTime FROM tb_seckill_order_outbox "
            + "WHERE status IN ('PENDING', 'SENT') AND next_retry_time <= NOW() "
            + "AND (relay_lease_until IS NULL OR relay_lease_until < NOW()) "
            + "ORDER BY id ASC LIMIT #{limit}")
    List<SeckillOrderOutboxEvent> findDispatchable(@Param("limit") int limit);

    @Select("SELECT id, event_id AS eventId, order_id AS orderId, voucher_id AS voucherId, "
            + "user_id AS userId, auto_issued AS autoIssued, status, retry_count AS retryCount, "
            + "next_retry_time AS nextRetryTime, last_error AS lastError, "
            + "relay_owner AS relayOwner, relay_lease_until AS relayLeaseUntil, "
            + "created_time AS createdTime, updated_time AS updatedTime, sent_time AS sentTime, "
            + "completed_time AS completedTime FROM tb_seckill_order_outbox "
            + "WHERE order_id = #{orderId} LIMIT 1")
    SeckillOrderOutboxEvent findByOrderId(@Param("orderId") Long orderId);

    @Update("UPDATE tb_seckill_order_outbox SET status = 'SENT', sent_time = NOW(), "
            + "next_retry_time = #{nextCheckTime}, updated_time = NOW(), last_error = NULL, "
            + "relay_owner = NULL, relay_lease_until = NULL WHERE id = #{id} "
            + "AND relay_owner = #{owner} AND status IN ('PENDING', 'SENT')")
    int markSent(@Param("id") Long id,
                 @Param("owner") String owner,
                 @Param("nextCheckTime") LocalDateTime nextCheckTime);

    @Update("UPDATE tb_seckill_order_outbox SET retry_count = retry_count + 1, "
            + "next_retry_time = #{nextRetryTime}, last_error = #{lastError}, "
            + "updated_time = NOW(), relay_owner = NULL, relay_lease_until = NULL "
            + "WHERE id = #{id} AND relay_owner = #{owner} AND status IN ('PENDING', 'SENT')")
    int scheduleRetry(@Param("id") Long id,
                      @Param("owner") String owner,
                      @Param("nextRetryTime") LocalDateTime nextRetryTime,
                      @Param("lastError") String lastError);

    @Update("UPDATE tb_seckill_order_outbox SET relay_owner = #{owner}, "
            + "relay_lease_until = #{leaseUntil}, updated_time = NOW() WHERE id = #{id} "
            + "AND status IN ('PENDING', 'SENT') AND next_retry_time <= NOW() "
            + "AND (relay_lease_until IS NULL OR relay_lease_until < NOW())")
    int claimRelay(@Param("id") Long id,
                   @Param("owner") String owner,
                   @Param("leaseUntil") LocalDateTime leaseUntil);

    @Update({"<script>",
            "UPDATE tb_seckill_order_outbox SET relay_owner = #{owner}, ",
            "relay_lease_until = #{leaseUntil}, updated_time = NOW() WHERE id IN ",
            "<foreach collection='ids' item='id' open='(' separator=',' close=')'>#{id}</foreach> ",
            "AND status IN ('PENDING', 'SENT') AND next_retry_time &lt;= NOW() ",
            "AND (relay_lease_until IS NULL OR relay_lease_until &lt; NOW())",
            "</script>"})
    int claimRelayBatch(@Param("ids") List<Long> ids,
                        @Param("owner") String owner,
                        @Param("leaseUntil") LocalDateTime leaseUntil);

    @Select({"<script>",
            "SELECT id, event_id AS eventId, order_id AS orderId, voucher_id AS voucherId, ",
            "user_id AS userId, auto_issued AS autoIssued, status, retry_count AS retryCount, ",
            "next_retry_time AS nextRetryTime, created_time AS createdTime, ",
            "updated_time AS updatedTime FROM tb_seckill_order_outbox ",
            "WHERE relay_owner = #{owner} AND id IN ",
            "<foreach collection='ids' item='id' open='(' separator=',' close=')'>#{id}</foreach>",
            "</script>"})
    List<SeckillOrderOutboxEvent> findClaimedBatch(
            @Param("ids") List<Long> ids, @Param("owner") String owner);

    @Update({"<script>",
            "UPDATE tb_seckill_order_outbox SET status = 'SENT', sent_time = NOW(), ",
            "next_retry_time = #{nextCheckTime}, updated_time = NOW(), last_error = NULL, ",
            "relay_owner = NULL, relay_lease_until = NULL WHERE relay_owner = #{owner} AND id IN ",
            "<foreach collection='ids' item='id' open='(' separator=',' close=')'>#{id}</foreach> ",
            "AND status IN ('PENDING', 'SENT')",
            "</script>"})
    int markSentBatch(@Param("ids") List<Long> ids,
                      @Param("owner") String owner,
                      @Param("nextCheckTime") LocalDateTime nextCheckTime);

    @Update({"<script>",
            "UPDATE tb_seckill_order_outbox SET retry_count = retry_count + 1, ",
            "next_retry_time = #{nextRetryTime}, last_error = #{lastError}, updated_time = NOW(), ",
            "relay_owner = NULL, relay_lease_until = NULL WHERE relay_owner = #{owner} AND id IN ",
            "<foreach collection='ids' item='id' open='(' separator=',' close=')'>#{id}</foreach> ",
            "AND status IN ('PENDING', 'SENT')",
            "</script>"})
    int scheduleRetryBatch(@Param("ids") List<Long> ids,
                           @Param("owner") String owner,
                           @Param("nextRetryTime") LocalDateTime nextRetryTime,
                           @Param("lastError") String lastError);

    @Update("UPDATE tb_seckill_order_outbox SET status = 'COMPLETED', "
            + "completed_time = COALESCE(completed_time, NOW()), "
            + "updated_time = NOW(), last_error = NULL, relay_owner = NULL, "
            + "relay_lease_until = NULL WHERE order_id = #{orderId} "
            + "AND status IN ('PENDING', 'SENT')")
    int markCompleted(@Param("orderId") Long orderId);

    @Update({"<script>",
            "UPDATE tb_seckill_order_outbox SET status = 'COMPLETED', ",
            "completed_time = COALESCE(completed_time, NOW()), ",
            "updated_time = NOW(), last_error = NULL, relay_owner = NULL, relay_lease_until = NULL ",
            "WHERE order_id IN ",
            "<foreach collection='orderIds' item='orderId' open='(' separator=',' close=')'>",
            "#{orderId}</foreach> AND status IN ('PENDING', 'SENT')",
            "</script>"})
    int markCompletedBatch(@Param("orderIds") List<Long> orderIds);

    @Select({"<script>",
            "SELECT id FROM tb_seckill_order_outbox WHERE order_id IN ",
            "<foreach collection='orderIds' item='orderId' open='(' separator=',' close=')'>",
            "#{orderId}</foreach> ORDER BY id ASC",
            "</script>"})
    List<Long> findIdsByOrderIds(@Param("orderIds") List<Long> orderIds);

    @Update({"<script>",
            "UPDATE tb_seckill_order_outbox SET status = 'COMPLETED', ",
            "completed_time = COALESCE(completed_time, NOW()), ",
            "updated_time = NOW(), last_error = NULL, relay_owner = NULL, relay_lease_until = NULL ",
            "WHERE id IN ",
            "<foreach collection='ids' item='id' open='(' separator=',' close=')'>",
            "#{id}</foreach> AND status IN ('PENDING', 'SENT')",
            "</script>"})
    int markCompletedBatchByIds(@Param("ids") List<Long> ids);

    @Select({"<script>",
            "SELECT COUNT(1) FROM tb_seckill_order_outbox WHERE status = 'COMPLETED' ",
            "AND id IN ",
            "<foreach collection='ids' item='id' open='(' separator=',' close=')'>",
            "#{id}</foreach>",
            "</script>"})
    int countCompletedBatchByIds(@Param("ids") List<Long> ids);

    @Select({"<script>",
            "SELECT COUNT(1) FROM tb_seckill_order_outbox WHERE status = 'COMPLETED' ",
            "AND order_id IN ",
            "<foreach collection='orderIds' item='orderId' open='(' separator=',' close=')'>",
            "#{orderId}</foreach>",
            "</script>"})
    int countCompletedBatch(@Param("orderIds") List<Long> orderIds);

    @Update("UPDATE tb_seckill_order_outbox SET status = 'MANUAL_REVIEW', updated_time = NOW(), "
            + "last_error = #{reason}, relay_owner = NULL, relay_lease_until = NULL "
            + "WHERE order_id = #{orderId} AND status <> 'COMPLETED'")
    int markManualReview(@Param("orderId") Long orderId, @Param("reason") String reason);

    @Update("UPDATE tb_seckill_order_outbox SET status = 'PENDING', "
            + "retry_count = retry_count + 1, next_retry_time = #{nextRetryTime}, "
            + "updated_time = NOW(), last_error = #{reason}, relay_owner = NULL, "
            + "relay_lease_until = NULL WHERE order_id = #{orderId} AND status <> 'COMPLETED'")
    int requeueAccepted(@Param("orderId") Long orderId,
                        @Param("nextRetryTime") LocalDateTime nextRetryTime,
                        @Param("reason") String reason);

    @Select("SELECT COUNT(1) FROM tb_seckill_order_outbox WHERE status IN ('PENDING', 'SENT')")
    long countBacklog();

    @Select("SELECT COUNT(1) FROM tb_seckill_order_outbox o WHERE o.voucher_id = #{voucherId} "
            + "AND o.status IN ('PENDING', 'SENT', 'MANUAL_REVIEW') "
            + "AND NOT EXISTS (SELECT 1 FROM tb_voucher_order vo WHERE vo.id = o.order_id)")
    int countUnpersistedReservations(@Param("voucherId") Long voucherId);

    @Select("SELECT DISTINCT o.user_id FROM tb_seckill_order_outbox o "
            + "WHERE o.voucher_id = #{voucherId} "
            + "AND o.status IN ('PENDING', 'SENT', 'MANUAL_REVIEW') "
            + "AND NOT EXISTS (SELECT 1 FROM tb_voucher_order vo WHERE vo.id = o.order_id)")
    List<Long> findUnpersistedUserIds(@Param("voucherId") Long voucherId);

    @Select("SELECT id, event_id AS eventId, order_id AS orderId, voucher_id AS voucherId, "
            + "user_id AS userId, auto_issued AS autoIssued, status, retry_count AS retryCount, "
            + "next_retry_time AS nextRetryTime, last_error AS lastError, "
            + "created_time AS createdTime, updated_time AS updatedTime, sent_time AS sentTime, "
            + "completed_time AS completedTime FROM tb_seckill_order_outbox o "
            + "WHERE o.voucher_id = #{voucherId} "
            + "AND o.status IN ('PENDING', 'SENT', 'MANUAL_REVIEW') "
            + "AND NOT EXISTS (SELECT 1 FROM tb_voucher_order vo WHERE vo.id = o.order_id)")
    List<SeckillOrderOutboxEvent> findUnpersistedEvents(@Param("voucherId") Long voucherId);

    @Select("SELECT id, event_id AS eventId, order_id AS orderId, voucher_id AS voucherId, "
            + "user_id AS userId, auto_issued AS autoIssued, status, retry_count AS retryCount, "
            + "next_retry_time AS nextRetryTime, last_error AS lastError, "
            + "relay_owner AS relayOwner, relay_lease_until AS relayLeaseUntil, "
            + "created_time AS createdTime, updated_time AS updatedTime, sent_time AS sentTime, "
            + "completed_time AS completedTime FROM tb_seckill_order_outbox "
            + "WHERE status = 'MANUAL_REVIEW' ORDER BY updated_time ASC LIMIT #{limit}")
    List<SeckillOrderOutboxEvent> findManualReview(@Param("limit") int limit);
}
