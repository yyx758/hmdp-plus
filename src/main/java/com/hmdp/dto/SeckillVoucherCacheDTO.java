package com.hmdp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;

/**
 * 秒杀券活动元数据缓存对象，不包含实时库存。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class SeckillVoucherCacheDTO {

    private Long voucherId;

    private Long shopId;

    private LocalDateTime beginTime;

    private LocalDateTime endTime;

    private Integer status;
}
