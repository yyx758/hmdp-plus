package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillOrderStatusDTO;
import com.hmdp.dto.SeckillVoucherCacheDTO;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.enums.SeckillOrderStatus;
import com.hmdp.exception.DatabaseStockMismatchException;
import com.hmdp.exception.OrderIdConflictException;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.IVoucherOrderPersistenceService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.SeckillVoucherCacheService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SeckillStreamReader;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private SeckillStreamReader seckillStreamReader;
    @Resource
    private IVoucherOrderPersistenceService voucherOrderPersistenceService;
    @Resource
    private SeckillVoucherCacheService seckillVoucherCacheService;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT = loadScript("seckill.lua");
    private static final DefaultRedisScript<Long> ORDER_CONFLICT_SCRIPT =
            loadScript("seckill_order_conflict.lua");

    private static DefaultRedisScript<Long> loadScript(String resourceName) {
        try {
            // 启动时一次性读取脚本文本，避免高并发执行时在 ResourceScriptSource.isModified() 上争抢 SHA1 检查锁
            String scriptText = StreamUtils.copyToString(
                    new ClassPathResource(resourceName).getInputStream(),
                    StandardCharsets.UTF_8
            );
            return new DefaultRedisScript<>(scriptText, Long.class);
        } catch (IOException e) {
            throw new IllegalStateException("加载秒杀 Lua 脚本失败：" + resourceName, e);
        }
    }


    @Value("${hmdp.seckill.consumer.worker-count:4}")
    private int consumerWorkerCount;

    @Value("${hmdp.seckill.consumer.instance-id:${server.port}}")
    private String consumerInstanceId;

    @Value("${hmdp.seckill.consumer.batch-size:100}")
    private int streamBatchSize;

    private ExecutorService seckillOrderExecutor;
    private volatile boolean running;
    private static final String STREAM_KEY = "stream.orders";
    private static final String STREAM_GROUP = "g1";
    private static final String STREAM_RETRY_COUNT_KEY = "stream.orders:retry";
    private static final String DEAD_LETTER_STREAM_KEY = "stream.orders.dlq";
    private static final String ORDER_RESULT_KEY_PREFIX = "seckill:order:result:";
    private static final long ORDER_RESULT_TTL_MILLIS = TimeUnit.DAYS.toMillis(1);
    private static final int MAX_RETRY_COUNT = 3;
    private static final long PENDING_CHECK_INTERVAL_MILLIS = 5000L;
    private static final long READ_FAILURE_BACKOFF_MILLIS = 1000L;

    @PostConstruct
    private void init() {
        if (consumerWorkerCount <= 0 || streamBatchSize <= 0) {
            throw new IllegalArgumentException("秒杀消费者线程数和批次大小必须大于0");
        }
        createStreamGroup();
        running = true;
        AtomicInteger threadSequence = new AtomicInteger();
        seckillOrderExecutor = Executors.newFixedThreadPool(consumerWorkerCount, runnable -> {
            Thread thread = new Thread(
                    runnable,
                    "seckill-order-consumer-" + threadSequence.incrementAndGet()
            );
            thread.setDaemon(false);
            return thread;
        });
        for (int workerIndex = 1; workerIndex <= consumerWorkerCount; workerIndex++) {
            seckillOrderExecutor.submit(new VoucherOrderHandler(buildConsumerName(workerIndex)));
        }
        log.info("秒杀订单消费者已启动，group={}，instanceId={}，workerCount={}，batchSize={}",
                STREAM_GROUP, consumerInstanceId, consumerWorkerCount, streamBatchSize);
    }

    @PreDestroy
    private void destroy() {
        running = false;
        if (seckillOrderExecutor == null) {
            return;
        }
        seckillOrderExecutor.shutdownNow();
        try {
            if (!seckillOrderExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                log.warn("秒杀订单消费者未在限定时间内停止");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void createStreamGroup() {
        try {
            stringRedisTemplate.opsForStream().createGroup(
                    STREAM_KEY,
                    ReadOffset.from("0"),
                    STREAM_GROUP
            );
        } catch (DataAccessException e) {
            if (!containsExceptionMessage(e, "BUSYGROUP")) {
                throw e;
            }
        }
    }

    private String buildConsumerName(int workerIndex) {
        return consumerInstanceId + "-worker-" + workerIndex;
    }

    private boolean containsExceptionMessage(Throwable cause, String expected) {
        Throwable current = cause;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().contains(expected)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private boolean isNoGroupException(Throwable cause) {
        return containsExceptionMessage(cause, "NOGROUP");
    }

    private void recoverStreamGroup() {
        try {
            createStreamGroup();
            log.warn("Redis Stream消费组不存在，已重新创建，stream={}, group={}",
                    STREAM_KEY, STREAM_GROUP);
        } catch (Exception e) {
            log.error("重新创建Redis Stream消费组失败，stream={}, group={}",
                    STREAM_KEY, STREAM_GROUP, e);
            backoffAfterReadFailure();
        }
    }

    private void backoffAfterReadFailure() {
        try {
            Thread.sleep(READ_FAILURE_BACKOFF_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private class VoucherOrderHandler implements Runnable {

        private final String consumerName;
        private long lastPendingCheckTime;

        private VoucherOrderHandler(String consumerName) {
            this.consumerName = consumerName;
        }

        @Override
        public void run() {
            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    // 定期检查pending-list，避免新消息持续到达时异常订单长期得不到重试
                    if (System.currentTimeMillis() - lastPendingCheckTime >= PENDING_CHECK_INTERVAL_MILLIS) {
                        checkPendingList();
                    }
                    // 1.使用当前消费者名称批量读取新消息，每条新消息只会分配给组内一个消费者
                    List<MapRecord<String, Object, Object>> list = seckillStreamReader.read(
                            Consumer.from(STREAM_GROUP, consumerName),
                            StreamReadOptions.empty().count(streamBatchSize).block(Duration.ofSeconds(2)),
                            StreamOffset.create(STREAM_KEY, ReadOffset.lastConsumed())
                    );
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有新消息，尝试处理pending-list
                        checkPendingList();
                        continue;
                    }
                    // 3.逐条处理本批订单，每条消息仍然在事务成功后单独ACK
                    processOrderBatch(list);
                } catch (Exception e) {
                    if (!running || Thread.currentThread().isInterrupted()) {
                        break;
                    }
                    if (isNoGroupException(e)) {
                        recoverStreamGroup();
                        continue;
                    }
                    log.error("读取订单消息异常", e);
                    checkPendingList();
                    backoffAfterReadFailure();
                }
            }
        }

        private void checkPendingList() {
            try {
                handlePendingList();
            } finally {
                lastPendingCheckTime = System.currentTimeMillis();
            }
        }

        private void handlePendingList() {
            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    // 1.只读取归属于当前消费者的pending消息，避免不同线程同时重试同一条消息
                    List<MapRecord<String, Object, Object>> list = seckillStreamReader.read(
                            Consumer.from(STREAM_GROUP, consumerName),
                            StreamReadOptions.empty().count(streamBatchSize),
                            StreamOffset.create(STREAM_KEY, ReadOffset.from("0"))
                    );
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有异常消息，结束循环
                        break;
                    }
                    // 3.本批消息各处理一次；只要存在失败消息，本轮扫描结束后等待下次定时重试
                    if (!processOrderBatch(list)) {
                        // 避免同一批异常消息在这里立即重复读取并空转
                        break;
                    }
                } catch (Exception e) {
                    if (!running || Thread.currentThread().isInterrupted()) {
                        break;
                    }
                    if (isNoGroupException(e)) {
                        recoverStreamGroup();
                    } else {
                        log.error("读取pending-list异常", e);
                        backoffAfterReadFailure();
                    }
                    break;
                }
            }
        }
    }

    private boolean processOrderBatch(List<MapRecord<String, Object, Object>> records) {
        if (records == null || records.isEmpty()) {
            return true;
        }

        List<VoucherOrder> orders = new java.util.ArrayList<>(records.size());
        try {
            for (MapRecord<String, Object, Object> record : records) {
                orders.add(toVoucherOrder(record));
            }
            voucherOrderPersistenceService.createVoucherOrders(orders);
        } catch (Exception batchException) {
            if (isBatchWideFailure(batchException)) {
                // 数据库整体不可用或库存整体不一致时，拆批只会放大故障并造成部分成功。
                log.error("订单整批落库失败，保留Pending等待恢复，batchSize={}", records.size(), batchException);
                retainBatchForRetry(
                        records,
                        batchException,
                        containsCause(batchException, DatabaseStockMismatchException.class)
                );
                backoffAfterReadFailure();
                return false;
            }
            return isolateFailedRecords(records, batchException);
        }

        try {
            // MySQL事务已经提交后先发布最终状态；状态写入失败时保留Pending，重试不会重复扣数据库库存。
            markOrdersSuccess(orders);
        } catch (Exception statusException) {
            log.error("订单已批量落库，但写入Redis成功状态失败，消息保留Pending，batchSize={}",
                    records.size(), statusException);
            return false;
        }

        try {
            acknowledgeBatch(records);
            return true;
        } catch (Exception ackException) {
            // 数据库事务已经提交。ACK失败时保持Pending，不能记为业务失败或转入死信。
            log.error("订单已批量落库，但Redis批量ACK失败，消息保留Pending，batchSize={}",
                    records.size(), ackException);
            return false;
        }
    }

    private boolean isolateFailedRecords(List<MapRecord<String, Object, Object>> records,
                                         Exception batchException) {
        if (records.size() == 1) {
            MapRecord<String, Object, Object> record = records.get(0);
            return handleProcessingFailure(record, record.getId().getValue(), batchException);
        }

        int middle = records.size() / 2;
        log.warn("订单批次包含可隔离异常，二分拆批继续处理，batchSize={}，leftSize={}，rightSize={}",
                records.size(), middle, records.size() - middle);
        boolean leftProcessed = processOrderBatch(records.subList(0, middle));
        boolean rightProcessed = processOrderBatch(records.subList(middle, records.size()));
        return leftProcessed && rightProcessed;
    }

    private boolean isBatchWideFailure(Throwable cause) {
        Throwable current = cause;
        while (current != null) {
            if (current instanceof DatabaseStockMismatchException
                    || current instanceof TransientDataAccessException
                    || current instanceof RecoverableDataAccessException) {
                return true;
            }
            if (current instanceof SQLException) {
                String sqlState = ((SQLException) current).getSQLState();
                if (sqlState != null && (sqlState.startsWith("08") || sqlState.startsWith("40"))) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private void retainBatchForRetry(List<MapRecord<String, Object, Object>> records,
                                     Exception cause,
                                     boolean allowFinalConsistencyCheck) {
        for (MapRecord<String, Object, Object> record : records) {
            String messageId = record.getId().getValue();
            try {
                Long retryCount = incrementRetryCount(messageId);
                if (allowFinalConsistencyCheck
                        && retryCount != null
                        && retryCount >= MAX_RETRY_COUNT) {
                    resolveAfterRetryLimit(record, messageId, retryCount, cause);
                }
            } catch (Exception retryException) {
                log.error("记录整批失败消息的重试状态异常，消息继续保留Pending，messageId={}",
                        messageId, retryException);
            }
        }
    }

    private boolean containsCause(Throwable cause, Class<? extends Throwable> expectedType) {
        Throwable current = cause;
        while (current != null) {
            if (expectedType.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private VoucherOrder toVoucherOrder(MapRecord<String, Object, Object> record) {
        return BeanUtil.fillBeanWithMap(record.getValue(), new VoucherOrder(), true);
    }

    private void acknowledgeBatch(List<MapRecord<String, Object, Object>> records) {
        RecordId[] recordIds = records.stream()
                .map(MapRecord::getId)
                .toArray(RecordId[]::new);
        stringRedisTemplate.opsForStream().acknowledge(STREAM_KEY, STREAM_GROUP, recordIds);
        Object[] messageIds = records.stream()
                .map(record -> record.getId().getValue())
                .toArray(Object[]::new);
        try {
            stringRedisTemplate.opsForHash().delete(STREAM_RETRY_COUNT_KEY, messageIds);
        } catch (Exception e) {
            // ACK已经完成，重试计数只是辅助信息，清理失败不能反向影响订单处理结果。
            log.warn("批量清理订单消息重试次数失败，batchSize={}", records.size(), e);
        }
    }

    /**
     * 处理订单消息。只有订单成功落库，或者消息成功转入死信队列后，才确认原消息。
     */
    private boolean processOrderRecord(MapRecord<String, Object, Object> record) {
        String messageId = record.getId().getValue();
        VoucherOrder voucherOrder;
        try {
            voucherOrder = toVoucherOrder(record);
            voucherOrderPersistenceService.createVoucherOrder(voucherOrder);
        } catch (Exception businessException) {
            return handleProcessingFailure(record, messageId, businessException);
        }

        try {
            // 订单成功落库（或数据库中已存在同一订单）后，先发布最终状态，再确认消息。
            markOrderSuccess(voucherOrder);
            stringRedisTemplate.opsForStream().acknowledge(STREAM_KEY, STREAM_GROUP, record.getId());
            clearRetryCount(messageId);
            return true;
        } catch (Exception ackException) {
            // ACK失败不是订单业务失败，不能增加业务重试次数或错误转入死信。
            log.error("订单已落库，但Redis ACK失败，消息保留Pending，messageId={}",
                    messageId, ackException);
            return false;
        }
    }

    private boolean handleProcessingFailure(MapRecord<String, Object, Object> record,
                                            String messageId,
                                            Exception cause) {
        if (containsCause(cause, OrderIdConflictException.class)) {
            // 这是永久业务冲突，重试相同INSERT不会恢复，直接进入最终一致性处理。
            return resolveAfterRetryLimit(record, messageId, 1L, cause);
        }
        Long retryCount = incrementRetryCount(messageId);
        long currentRetryCount = retryCount == null ? 1L : retryCount;

        if (currentRetryCount < MAX_RETRY_COUNT) {
            log.warn("订单消息处理失败，messageId={}，第{}次尝试，保留在pending-list等待重试",
                    messageId, currentRetryCount, cause);
            return false;
        }

        return resolveAfterRetryLimit(record, messageId, currentRetryCount, cause);
    }

    private Long incrementRetryCount(String messageId) {
        return stringRedisTemplate.opsForHash()
                .increment(STREAM_RETRY_COUNT_KEY, messageId, 1L);
    }

    private boolean resolveAfterRetryLimit(MapRecord<String, Object, Object> record,
                                           String messageId,
                                           long retryCount,
                                           Exception cause) {
        VoucherOrder voucherOrder;
        try {
            voucherOrder = toVoucherOrder(record);
            if (voucherOrder.getId() == null || voucherOrder.getUserId() == null
                    || voucherOrder.getVoucherId() == null) {
                throw new IllegalArgumentException("消息缺少id、userId或voucherId");
            }
        } catch (Exception invalidMessageException) {
            return deadLetterAndAcknowledge(
                    record, messageId, retryCount, cause,
                    "SKIPPED_INVALID_MESSAGE", null, null
            );
        }

        try {
            Long existingOrderId = voucherOrderPersistenceService.findOrderId(
                    voucherOrder.getUserId(), voucherOrder.getVoucherId()
            );
            String orderKey = "seckill:order:" + voucherOrder.getVoucherId();
            Boolean redisPurchaseMarked = stringRedisTemplate.opsForSet().isMember(
                    orderKey, voucherOrder.getUserId().toString()
            );
            if (redisPurchaseMarked == null) {
                throw new IllegalStateException("读取Redis购买标记返回空结果");
            }

            if (existingOrderId != null) {
                if (!voucherOrder.getId().equals(existingOrderId)) {
                    return compensateOrderIdConflictAndAcknowledge(
                            record, voucherOrder, existingOrderId, retryCount, cause,
                            redisPurchaseMarked
                    );
                }
                if (!redisPurchaseMarked) {
                    Long added = stringRedisTemplate.opsForSet().add(
                            orderKey, voucherOrder.getUserId().toString()
                    );
                    if (added == null) {
                        throw new IllegalStateException("恢复Redis购买标记失败");
                    }
                    log.error("检测到MySQL订单存在但Redis购买标记缺失，已恢复标记，messageId={}，orderId={}",
                            messageId, existingOrderId);
                }
                markOrderSuccess(voucherOrder);
                stringRedisTemplate.opsForStream().acknowledge(
                        STREAM_KEY, STREAM_GROUP, record.getId()
                );
                clearRetryCount(messageId);
                log.warn("失败消息最终校验发现MySQL订单已存在，按幂等成功ACK，messageId={}，orderId={}",
                        messageId, existingOrderId);
                return true;
            }

            return deadLetterAndAcknowledge(
                    record, messageId, retryCount, cause,
                    "CHECKED_ORDER_MISSING", false, redisPurchaseMarked
            );
        } catch (Exception consistencyException) {
            // 无法完成最终一致性检查时不能ACK，否则可能永久丢失尚未落库的订单。
            log.error("失败消息最终一致性校验异常，消息继续保留Pending，messageId={}",
                    messageId, consistencyException);
            return false;
        }
    }

    private boolean compensateOrderIdConflictAndAcknowledge(
            MapRecord<String, Object, Object> record,
            VoucherOrder voucherOrder,
            Long existingOrderId,
            long retryCount,
            Exception cause,
            Boolean redisPurchaseMarked) {
        Long compensated = stringRedisTemplate.execute(
                ORDER_CONFLICT_SCRIPT,
                Collections.emptyList(),
                voucherOrder.getVoucherId().toString(),
                voucherOrder.getUserId().toString(),
                voucherOrder.getId().toString(),
                existingOrderId.toString(),
                record.getId().getValue(),
                String.valueOf(System.currentTimeMillis()),
                String.valueOf(ORDER_RESULT_TTL_MILLIS)
        );
        if (compensated == null) {
            throw new IllegalStateException("执行订单ID冲突补偿脚本未返回结果");
        }
        if (compensated < 0) {
            return deadLetterAndAcknowledge(
                    record, record.getId().getValue(), retryCount, cause,
                    "ORDER_ID_CONFLICT_STOCK_MISSING", true, redisPurchaseMarked
            );
        }
        log.error("检测到Redis生成了重复业务订单，已幂等回补Redis库存并返回原订单，messageId={}，currentOrderId={}，existingOrderId={}，firstCompensation={}",
                record.getId().getValue(), voucherOrder.getId(), existingOrderId, compensated == 1L);
        return true;
    }

    private boolean deadLetterAndAcknowledge(MapRecord<String, Object, Object> record,
                                             String messageId,
                                             long retryCount,
                                             Exception cause,
                                             String consistencyStatus,
                                             Boolean mysqlOrderExists,
                                             Boolean redisPurchaseMarked) {
        try {
            moveToDeadLetter(
                    record, retryCount, cause,
                    consistencyStatus, mysqlOrderExists, redisPurchaseMarked
            );
            VoucherOrder voucherOrder = null;
            try {
                voucherOrder = toVoucherOrder(record);
            } catch (Exception invalidMessageException) {
                // 无法解析出订单ID时没有可更新的查询状态，但死信记录本身仍然有效。
                log.warn("死信消息无法解析订单状态字段，跳过状态写入，messageId={}",
                        messageId, invalidMessageException);
            }
            if (voucherOrder != null && voucherOrder.getId() != null && voucherOrder.getUserId() != null
                    && voucherOrder.getVoucherId() != null) {
                markOrderStatus(
                        voucherOrder,
                        SeckillOrderStatus.MANUAL_REVIEW,
                        null,
                        "订单处理异常，已进入人工核对"
                );
            }
            // 必须先成功写入死信队列，才能确认原消息。
            stringRedisTemplate.opsForStream().acknowledge(
                    STREAM_KEY, STREAM_GROUP, record.getId()
            );
            clearRetryCount(messageId);
            log.error("订单消息连续处理失败，最终校验后已转入死信队列，messageId={}，retryCount={}，status={}",
                    messageId, retryCount, consistencyStatus, cause);
            return true;
        } catch (Exception deadLetterException) {
            log.error("订单消息写入死信队列或ACK失败，原消息保留Pending，messageId={}",
                    messageId, deadLetterException);
            return false;
        }
    }

    private void moveToDeadLetter(MapRecord<String, Object, Object> record,
                                  long retryCount,
                                  Exception cause,
                                  String consistencyStatus,
                                  Boolean mysqlOrderExists,
                                  Boolean redisPurchaseMarked) {
        Map<Object, Object> deadLetter = new LinkedHashMap<>(record.getValue());
        deadLetter.put("originalMessageId", record.getId().getValue());
        deadLetter.put("retryCount", String.valueOf(retryCount));
        deadLetter.put("failedAt", String.valueOf(System.currentTimeMillis()));
        deadLetter.put("error", summarizeError(cause));
        deadLetter.put("consistencyStatus", consistencyStatus);
        if (mysqlOrderExists != null) {
            deadLetter.put("mysqlOrderExists", mysqlOrderExists.toString());
        }
        if (redisPurchaseMarked != null) {
            deadLetter.put("redisPurchaseMarked", redisPurchaseMarked.toString());
        }

        RecordId deadLetterId = stringRedisTemplate.opsForStream().add(
                StreamRecords.mapBacked(deadLetter).withStreamKey(DEAD_LETTER_STREAM_KEY)
        );
        if (deadLetterId == null) {
            throw new IllegalStateException("死信消息写入失败");
        }
    }

    private void clearRetryCount(String messageId) {
        try {
            stringRedisTemplate.opsForHash().delete(STREAM_RETRY_COUNT_KEY, messageId);
        } catch (Exception e) {
            // 原消息已经确认，重试计数只是辅助信息，清理失败不能反向影响订单处理结果
            log.warn("清理订单消息重试次数失败，messageId={}", messageId, e);
        }
    }

    private String summarizeError(Exception cause) {
        String message = cause.getMessage();
        String error = cause.getClass().getSimpleName() + (message == null ? "" : ": " + message);
        return error.length() <= 500 ? error : error.substring(0, 500);
    }

    private void markOrdersSuccess(List<VoucherOrder> orders) {
        final long updatedAt = System.currentTimeMillis();
        stringRedisTemplate.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings({"unchecked", "rawtypes"})
            public <K, V> Object execute(RedisOperations<K, V> operations) throws DataAccessException {
                RedisOperations<String, String> redisOperations = (RedisOperations) operations;
                for (VoucherOrder order : orders) {
                    String resultKey = buildOrderResultKey(order.getId());
                    redisOperations.opsForHash().putAll(
                            resultKey,
                            buildStatusFields(
                                    order,
                                    SeckillOrderStatus.SUCCESS,
                                    null,
                                    "订单创建成功",
                                    updatedAt
                            )
                    );
                    redisOperations.expire(
                            resultKey,
                            ORDER_RESULT_TTL_MILLIS,
                            TimeUnit.MILLISECONDS
                    );
                }
                return null;
            }
        });
    }

    private void markOrderSuccess(VoucherOrder voucherOrder) {
        markOrderStatus(
                voucherOrder,
                SeckillOrderStatus.SUCCESS,
                null,
                "订单创建成功"
        );
    }

    private void markOrderStatus(VoucherOrder voucherOrder,
                                 SeckillOrderStatus status,
                                 Long existingOrderId,
                                 String message) {
        String resultKey = buildOrderResultKey(voucherOrder.getId());
        stringRedisTemplate.opsForHash().putAll(
                resultKey,
                buildStatusFields(
                        voucherOrder,
                        status,
                        existingOrderId,
                        message,
                        System.currentTimeMillis()
                )
        );
        stringRedisTemplate.expire(
                resultKey,
                ORDER_RESULT_TTL_MILLIS,
                TimeUnit.MILLISECONDS
        );
    }

    private Map<Object, Object> buildStatusFields(VoucherOrder voucherOrder,
                                                   SeckillOrderStatus status,
                                                   Long existingOrderId,
                                                   String message,
                                                   long updatedAt) {
        Map<Object, Object> fields = new LinkedHashMap<>();
        fields.put("orderId", voucherOrder.getId().toString());
        fields.put("userId", voucherOrder.getUserId().toString());
        fields.put("voucherId", voucherOrder.getVoucherId().toString());
        fields.put("status", status.name());
        fields.put("message", message);
        fields.put("updatedAt", String.valueOf(updatedAt));
        if (existingOrderId != null) {
            fields.put("existingOrderId", existingOrderId.toString());
        }
        return fields;
    }

    private String buildOrderResultKey(Long orderId) {
        return ORDER_RESULT_KEY_PREFIX + orderId;
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 多级缓存用于存在性与活动状态的前置过滤；Redis Lua 仍做最终一致性校验。
        SeckillVoucherCacheDTO voucher = seckillVoucherCacheService.queryById(voucherId);
        if (voucher == null) {
            return Result.fail("秒杀活动配置不存在");
        }
        LocalDateTime now = LocalDateTime.now();
        if (voucher.getStatus() == null || voucher.getStatus() != 1) {
            return Result.fail("秒杀券已下架或不可用");
        }
        if (voucher.getBeginTime() == null || now.isBefore(voucher.getBeginTime())) {
            return Result.fail("秒杀尚未开始");
        }
        if (voucher.getEndTime() == null || now.isAfter(voucher.getEndTime())) {
            return Result.fail("秒杀已经结束");
        }
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId),
                String.valueOf(System.currentTimeMillis()),
                String.valueOf(ORDER_RESULT_TTL_MILLIS)
        );
        if (result == null) {
            return Result.fail("秒杀服务繁忙，请稍后重试");
        }
        int r = result.intValue();
        // 2.判断结果是否为0
        if (r != 0) {
            // 2.1.不为0 ，代表没有购买资格
            String message;
            switch (r) {
                case 1:
                    message = "库存不足";
                    break;
                case 2:
                    message = "不能重复下单";
                    break;
                case 3:
                    message = "秒杀活动配置不存在";
                    break;
                case 4:
                    message = "秒杀尚未开始";
                    break;
                case 5:
                    message = "秒杀已经结束";
                    break;
                case 6:
                    message = "秒杀券已下架或不可用";
                    break;
                default:
                    message = "秒杀失败";
                    break;
            }
            return Result.fail(message);
        }
        // 3.这里只表示请求已受理，最终结果由消费者落库后更新，前端应通过状态接口轮询。
        return Result.ok(new SeckillOrderStatusDTO()
                .setOrderId(orderId)
                .setStatus(SeckillOrderStatus.PROCESSING.name())
                .setMessage("抢购请求已受理，订单正在处理中"));
    }

    @Override
    public Result querySeckillOrderStatus(Long orderId) {
        if (orderId == null) {
            return Result.fail("订单ID不能为空");
        }
        Long currentUserId = UserHolder.getUser().getId();
        Map<Object, Object> statusFields = stringRedisTemplate.opsForHash()
                .entries(buildOrderResultKey(orderId));

        if (statusFields == null || statusFields.isEmpty()) {
            // Redis状态过期后仍可用MySQL订单兜底查询真正成功的订单。
            VoucherOrder persistedOrder = getById(orderId);
            if (persistedOrder == null || !currentUserId.equals(persistedOrder.getUserId())) {
                return Result.fail("订单处理结果不存在或已过期");
            }
            return Result.ok(new SeckillOrderStatusDTO()
                    .setOrderId(orderId)
                    .setStatus(SeckillOrderStatus.SUCCESS.name())
                    .setMessage("订单创建成功"));
        }

        Long ownerUserId = parseLong(statusFields.get("userId"));
        if (!currentUserId.equals(ownerUserId)) {
            return Result.fail("订单处理结果不存在或已过期");
        }

        return Result.ok(new SeckillOrderStatusDTO()
                .setOrderId(orderId)
                .setStatus(stringValue(statusFields.get("status")))
                .setExistingOrderId(parseLong(statusFields.get("existingOrderId")))
                .setMessage(stringValue(statusFields.get("message"))));
    }

    private Long parseLong(Object value) {
        if (value == null) {
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

}
