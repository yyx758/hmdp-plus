package com.hmdp.controller;

import com.hmdp.dto.Result;
import com.hmdp.service.IUserNotificationService;
import com.hmdp.utils.UserHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@RestController
@RequestMapping("/notifications")
public class UserNotificationController {

    @Resource
    private IUserNotificationService userNotificationService;

    @GetMapping
    public Result queryLatest(@RequestParam(value = "limit", defaultValue = "20") int limit) {
        return Result.ok(userNotificationService.queryLatest(
                UserHolder.getUser().getId(), limit));
    }

    @GetMapping("unread-count")
    public Result countUnread() {
        return Result.ok(userNotificationService.countUnread(
                UserHolder.getUser().getId()));
    }

    @PostMapping("{notificationId}/read")
    public Result markRead(@PathVariable("notificationId") Long notificationId) {
        userNotificationService.markRead(UserHolder.getUser().getId(), notificationId);
        return Result.ok();
    }

    @PostMapping("read-all")
    public Result markAllRead() {
        userNotificationService.markAllRead(UserHolder.getUser().getId());
        return Result.ok();
    }
}
