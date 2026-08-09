package com.hmdp.annotation;

import com.hmdp.enums.CacheLockMode;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

/**
 * Coordinates cache reads and writes for the same business key across application instances.
 *
 * <p>The aspect has a higher precedence than Spring transactions, so a write lock remains held
 * until the database transaction and its AFTER_COMMIT cache invalidation have completed.</p>
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CacheConsistencyLock {

    String name();

    /** Spring Expression Language, for example {@code #p0} or {@code #p0.id}. */
    String key();

    CacheLockMode mode() default CacheLockMode.READ;

    long waitTime() default 3L;

    TimeUnit timeUnit() default TimeUnit.SECONDS;
}
