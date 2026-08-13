package com.hmdp.service;

import com.hmdp.kafka.outbox.SeckillOrderOutboxEvent;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;

import java.util.Set;
import java.util.Collections;
import java.util.List;

import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_HANDOFF_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_ACCEPTED_KEY;

@Component
public class SeckillOrderHandoffService {

    private static final DefaultRedisScript<Long> COMPLETE_SCRIPT = loadScript(
            "lua/seckill_handoff_complete.lua");

    private final StringRedisTemplate stringRedisTemplate;

    public SeckillOrderHandoffService(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public String buildKey(Long voucherId) {
        return SECKILL_ORDER_HANDOFF_KEY + "{" + voucherId + "}";
    }

    public String buildMember(Long orderId, Long userId, boolean autoIssued) {
        return orderId + "|" + userId + "|" + (autoIssued ? "1" : "0");
    }

    public void remove(SeckillOrderOutboxEvent event) {
        stringRedisTemplate.opsForZSet().remove(
                buildKey(event.getVoucherId()),
                buildMember(event.getOrderId(), event.getUserId(),
                        Boolean.TRUE.equals(event.getAutoIssued())));
    }

    public Set<String> findFirst(Long voucherId, int limit) {
        if (limit <= 0) {
            return Collections.emptySet();
        }
        return stringRedisTemplate.opsForZSet().range(
                buildKey(voucherId), 0, limit - 1L);
    }

    public long removeBatch(Long voucherId, Set<String> members) {
        if (members == null || members.isEmpty()) {
            return 0L;
        }
        Long removed = stringRedisTemplate.opsForZSet().remove(
                buildKey(voucherId), members.toArray(new String[0]));
        return removed == null ? 0L : removed;
    }

    public long completeBatch(
            Long voucherId, List<SeckillOrderOutboxEvent> events, Set<String> members) {
        if (events == null || events.isEmpty() || members == null || members.isEmpty()) {
            return 0L;
        }
        List<String> arguments = new java.util.ArrayList<>(events.size() * 2);
        java.util.Iterator<String> memberIterator = members.iterator();
        for (SeckillOrderOutboxEvent event : events) {
            if (!memberIterator.hasNext()) {
                throw new IllegalArgumentException("Handoff events and members are inconsistent");
            }
            arguments.add(memberIterator.next());
            arguments.add(event.getOrderId().toString());
        }
        Long removed = stringRedisTemplate.execute(
                COMPLETE_SCRIPT,
                java.util.Arrays.asList(buildKey(voucherId), SECKILL_ORDER_ACCEPTED_KEY),
                arguments.toArray(new String[0]));
        return removed == null ? 0L : removed;
    }

    public Set<String> findExpired(Long voucherId, long maxScore, int limit) {
        return stringRedisTemplate.opsForZSet().rangeByScore(
                buildKey(voucherId), 0, maxScore, 0, limit);
    }

    public Set<String> findAll(Long voucherId) {
        return stringRedisTemplate.opsForZSet().range(buildKey(voucherId), 0, -1);
    }

    public SeckillOrderOutboxEvent parse(Long voucherId, String member) {
        if (voucherId == null || member == null) {
            return null;
        }
        String[] parts = member.split("\\|", -1);
        if (parts.length != 3) {
            return null;
        }
        try {
            return SeckillOrderOutboxEvent.pending(
                    Long.valueOf(parts[0]), voucherId, Long.valueOf(parts[1]),
                    "1".equals(parts[2]));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static DefaultRedisScript<Long> loadScript(String resource) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(new ClassPathResource(resource)));
        script.setResultType(Long.class);
        return script;
    }
}
