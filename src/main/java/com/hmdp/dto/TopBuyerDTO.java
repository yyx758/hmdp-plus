package com.hmdp.dto;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class TopBuyerDTO {

    private Long userId;

    private String nickName;

    private String icon;

    private Integer level;

    private Double score;
}
