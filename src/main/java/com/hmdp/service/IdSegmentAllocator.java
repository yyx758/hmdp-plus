package com.hmdp.service;

import com.hmdp.entity.IdSegment;
import com.hmdp.mapper.IdSegmentMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class IdSegmentAllocator {

    private final IdSegmentMapper mapper;

    public IdSegmentAllocator(IdSegmentMapper mapper) {
        this.mapper = mapper;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Range allocate(String bizTag, int configuredStep) {
        IdSegment segment = mapper.lockByBizTag(bizTag);
        if (segment == null) {
            throw new IllegalStateException("ID segment is not configured: " + bizTag);
        }
        int step = configuredStep > 0 ? configuredStep : segment.getStep();
        if (step <= 0 || segment.getMaxId() > Long.MAX_VALUE - step) {
            throw new IllegalStateException("ID segment is exhausted: " + bizTag);
        }
        if (mapper.advance(bizTag, step) != 1) {
            throw new IllegalStateException("Failed to advance ID segment: " + bizTag);
        }
        return new Range(segment.getMaxId(), segment.getMaxId() + step);
    }

    public static final class Range {
        private final long startInclusive;
        private final long endExclusive;

        public Range(long startInclusive, long endExclusive) {
            this.startInclusive = startInclusive;
            this.endExclusive = endExclusive;
        }

        public long getStartInclusive() {
            return startInclusive;
        }

        public long getEndExclusive() {
            return endExclusive;
        }
    }
}
