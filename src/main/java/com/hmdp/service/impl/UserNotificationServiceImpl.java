package com.hmdp.service.impl;

import com.hmdp.config.SeckillMarketingProperties;
import com.hmdp.dto.UserNotificationDTO;
import com.hmdp.service.IUserNotificationService;
import com.hmdp.utils.RedisIdWorker;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.USER_NOTIFICATION_KEY;

@Service
public class UserNotificationServiceImpl implements IUserNotificationService {

    private static final DefaultRedisScript<Long> PUBLISH_SCRIPT = loadPublishScript();

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private SeckillMarketingProperties properties;

    @Override
    public boolean publish(
            Long userId,
            String type,
            String title,
            String content,
            Long voucherId,
            String deduplicationKey) {
        if (userId == null || type == null || title == null || content == null) {
            throw new IllegalArgumentException("通知参数不完整");
        }
        long notificationId = redisIdWorker.nextId("notification");
        long now = System.currentTimeMillis();
        long retentionMillis = TimeUnit.DAYS.toMillis(
                Math.max(1, properties.getNotification().getRetentionDays()));
        long dedupMillis = TimeUnit.SECONDS.toMillis(
                Math.max(1, properties.getNotification().getDedupSeconds()));
        String prefix = buildUserPrefix(userId);
        String dedupSuffix = deduplicationKey == null
                ? String.valueOf(notificationId) : deduplicationKey;
        Long result = stringRedisTemplate.execute(
                PUBLISH_SCRIPT,
                Arrays.asList(
                        prefix + "dedup:" + dedupSuffix,
                        prefix + "detail:" + notificationId,
                        prefix + "inbox",
                        prefix + "unread"),
                String.valueOf(notificationId),
                type,
                title,
                content,
                voucherId == null ? "" : voucherId.toString(),
                String.valueOf(now),
                String.valueOf(dedupMillis),
                String.valueOf(retentionMillis),
                String.valueOf(Math.max(1, properties.getNotification().getMaxItems())));
        return Long.valueOf(1L).equals(result);
    }

    @Override
    public List<UserNotificationDTO> queryLatest(Long userId, int limit) {
        if (userId == null) {
            return Collections.emptyList();
        }
        String prefix = buildUserPrefix(userId);
        Set<String> ids = stringRedisTemplate.opsForZSet().reverseRange(
                prefix + "inbox", 0, Math.max(1, Math.min(limit, 100)) - 1L);
        if (ids == null || ids.isEmpty()) {
            return Collections.emptyList();
        }
        List<UserNotificationDTO> notifications = new ArrayList<>(ids.size());
        for (String id : ids) {
            Map<Object, Object> fields = stringRedisTemplate.opsForHash()
                    .entries(prefix + "detail:" + id);
            if (fields == null || fields.isEmpty()) {
                continue;
            }
            notifications.add(toDTO(fields));
        }
        return notifications;
    }

    @Override
    public long countUnread(Long userId) {
        if (userId == null) {
            return 0L;
        }
        Long size = stringRedisTemplate.opsForSet().size(buildUserPrefix(userId) + "unread");
        return size == null ? 0L : size;
    }

    @Override
    public void markRead(Long userId, Long notificationId) {
        if (userId == null || notificationId == null) {
            return;
        }
        String prefix = buildUserPrefix(userId);
        stringRedisTemplate.opsForHash().put(
                prefix + "detail:" + notificationId, "read", "1");
        stringRedisTemplate.opsForSet().remove(prefix + "unread", notificationId.toString());
    }

    @Override
    public void markAllRead(Long userId) {
        if (userId == null) {
            return;
        }
        String prefix = buildUserPrefix(userId);
        Set<String> unreadIds = stringRedisTemplate.opsForSet().members(prefix + "unread");
        if (unreadIds == null || unreadIds.isEmpty()) {
            return;
        }
        stringRedisTemplate.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings({"unchecked", "rawtypes"})
            public <K, V> Object execute(RedisOperations<K, V> operations) {
                RedisOperations<String, String> redisOperations = (RedisOperations) operations;
                for (String id : unreadIds) {
                    redisOperations.opsForHash().put(prefix + "detail:" + id, "read", "1");
                }
                redisOperations.delete(prefix + "unread");
                return null;
            }
        });
    }

    private UserNotificationDTO toDTO(Map<Object, Object> fields) {
        return new UserNotificationDTO()
                .setId(parseLong(fields.get("id")))
                .setType(stringValue(fields.get("type")))
                .setTitle(stringValue(fields.get("title")))
                .setContent(stringValue(fields.get("content")))
                .setVoucherId(parseLong(fields.get("voucherId")))
                .setRead("1".equals(stringValue(fields.get("read"))))
                .setCreateTime(parseLong(fields.get("createTime")));
    }

    private String buildUserPrefix(Long userId) {
        return USER_NOTIFICATION_KEY + "{" + userId + "}:";
    }

    private Long parseLong(Object value) {
        if (value == null || value.toString().isEmpty()) {
            return null;
        }
        try {
            return Long.valueOf(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private String stringValue(Object value) {
        return value == null ? null : value.toString();
    }

    private static DefaultRedisScript<Long> loadPublishScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(
                new ClassPathResource("lua/notification_publish.lua")));
        script.setResultType(Long.class);
        return script;
    }
}
