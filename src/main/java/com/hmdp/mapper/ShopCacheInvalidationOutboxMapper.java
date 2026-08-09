package com.hmdp.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hmdp.kafka.outbox.ShopCacheInvalidationOutboxEvent;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;
import java.util.List;

public interface ShopCacheInvalidationOutboxMapper
        extends BaseMapper<ShopCacheInvalidationOutboxEvent> {

    @Select("SELECT id, event_id AS eventId, shop_id AS shopId, reason, status, "
            + "retry_count AS retryCount, next_retry_time AS nextRetryTime, "
            + "last_error AS lastError, created_time AS createdTime, "
            + "updated_time AS updatedTime, sent_time AS sentTime "
            + "FROM tb_shop_cache_invalidation_outbox "
            + "WHERE status = 'PENDING' AND next_retry_time <= NOW() "
            + "ORDER BY id ASC LIMIT #{limit}")
    List<ShopCacheInvalidationOutboxEvent> findDispatchable(@Param("limit") int limit);

    @Update("UPDATE tb_shop_cache_invalidation_outbox "
            + "SET status = 'SENT', sent_time = NOW(), updated_time = NOW(), last_error = NULL "
            + "WHERE id = #{id} AND status = 'PENDING'")
    int markSent(@Param("id") Long id);

    @Update("UPDATE tb_shop_cache_invalidation_outbox "
            + "SET retry_count = retry_count + 1, next_retry_time = #{nextRetryTime}, "
            + "last_error = #{lastError}, updated_time = NOW() "
            + "WHERE id = #{id} AND status = 'PENDING'")
    int scheduleRetry(@Param("id") Long id,
                      @Param("nextRetryTime") LocalDateTime nextRetryTime,
                      @Param("lastError") String lastError);

    @Update("UPDATE tb_shop_cache_invalidation_outbox "
            + "SET status = 'FAILED', retry_count = retry_count + 1, "
            + "last_error = #{lastError}, updated_time = NOW() "
            + "WHERE id = #{id} AND status = 'PENDING'")
    int markFailed(@Param("id") Long id, @Param("lastError") String lastError);
}
