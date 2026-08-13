package com.hmdp.service;

import com.hmdp.dto.UserNotificationDTO;

import java.util.List;

public interface IUserNotificationService {

    boolean publish(
            Long userId,
            String type,
            String title,
            String content,
            Long voucherId,
            String deduplicationKey);

    List<UserNotificationDTO> queryLatest(Long userId, int limit);

    long countUnread(Long userId);

    void markRead(Long userId, Long notificationId);

    void markAllRead(Long userId);
}
