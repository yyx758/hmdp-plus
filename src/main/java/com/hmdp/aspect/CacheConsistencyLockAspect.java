package com.hmdp.aspect;

import com.hmdp.annotation.CacheConsistencyLock;
import com.hmdp.enums.CacheLockMode;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RedissonClient;
import org.springframework.core.annotation.Order;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;

import static com.hmdp.utils.RedisConstants.LOCK_CACHE_CONSISTENCY_KEY;

/**
 * Applies a Redisson read/write lock before Spring's transaction interceptor.
 */
@Aspect
@Component
@Order(-10)
public class CacheConsistencyLockAspect {

    private final RedissonClient redissonClient;
    private final ExpressionParser expressionParser = new SpelExpressionParser();

    public CacheConsistencyLockAspect(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    @Around("@annotation(cacheConsistencyLock)")
    public Object around(ProceedingJoinPoint joinPoint,
                         CacheConsistencyLock cacheConsistencyLock) throws Throwable {
        Object businessKey = resolveBusinessKey(joinPoint.getArgs(), cacheConsistencyLock.key());
        if (businessKey == null) {
            throw new IllegalArgumentException("Cache consistency lock key must not be null");
        }

        String lockKey = LOCK_CACHE_CONSISTENCY_KEY
                + cacheConsistencyLock.name()
                + ":"
                + businessKey;
        RReadWriteLock readWriteLock = redissonClient.getReadWriteLock(lockKey);
        RLock lock = cacheConsistencyLock.mode() == CacheLockMode.WRITE
                ? readWriteLock.writeLock()
                : readWriteLock.readLock();

        boolean acquired;
        try {
            acquired = lock.tryLock(cacheConsistencyLock.waitTime(), cacheConsistencyLock.timeUnit());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while acquiring cache consistency lock: " + lockKey, e);
        }
        if (!acquired) {
            throw new IllegalStateException("Timed out acquiring cache consistency lock: " + lockKey);
        }

        try {
            return joinPoint.proceed();
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private Object resolveBusinessKey(Object[] arguments, String expression) {
        EvaluationContext context = new StandardEvaluationContext();
        for (int index = 0; index < arguments.length; index++) {
            context.setVariable("p" + index, arguments[index]);
            context.setVariable("a" + index, arguments[index]);
        }
        return expressionParser.parseExpression(expression).getValue(context);
    }
}
