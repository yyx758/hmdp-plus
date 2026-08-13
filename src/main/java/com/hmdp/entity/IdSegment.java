package com.hmdp.entity;

import lombok.Data;

@Data
public class IdSegment {
    private String bizTag;
    private Long maxId;
    private Integer step;
}
