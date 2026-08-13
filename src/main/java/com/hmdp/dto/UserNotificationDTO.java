package com.hmdp.dto;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class UserNotificationDTO {

    private Long id;

    private String type;

    private String title;

    private String content;

    private Long voucherId;

    private Boolean read;

    private Long createTime;
}
