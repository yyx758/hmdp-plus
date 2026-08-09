package com.hmdp.dto;

import lombok.Data;

import java.time.LocalDateTime;

/**
 * 秒杀券活动信息修改参数。
 *
 * <p>实时库存不允许通过普通编辑接口覆盖，避免把 Redis 中已经扣减的库存写回旧值。</p>
 */
@Data
public class SeckillVoucherUpdateDTO {

    private Long voucherId;

    private String title;

    private String subTitle;

    private String rules;

    private Long payValue;

    private Long actualValue;

    /**
     * 1：上架；2：下架；3：过期。
     */
    private Integer status;

    private LocalDateTime beginTime;

    private LocalDateTime endTime;
}
