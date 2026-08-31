# 黑马点评 Plus 升级版 — 8 大面试题对照分析

> 对比对象：当前 hm-dianping 项目 vs hmdp-plushmdp-plus 参考实现
> 分析时间：2026-08-13

---

## 汇总对照表

| # | 面试题 | 当前项目 | 实现方式 | 完善程度 |
|---|--------|----------|----------|----------|
| 1 | 流量突刺时，如何动态限流？ | ✅ 已实现 | Lua 令牌桶 + IP/用户双重限流 + 用户等级乘数 | 完整 |
| 2 | Redis 宕机了怎么办？ | ✅ 已实现 | Redis Sentinel 1主2从+3哨兵自动故障转移 + 缓存降级 + 启动自动恢复 | 完整 |
| 3 | Redis 数据丢失了怎么办？ | ✅ 已实现 | 启动时 `setIfAbsent` 补建 Stock Key + Bloom Filter 重建 + 元数据同步 | 完整 |
| 4 | MQ 宕机了怎么办？ | ✅ 已实现 | Kafka 多副本 + `min.insync.replicas` + Outbox 表兜底 + MySQL 租约继电器 | 完整 |
| 5 | MQ 消息丢失了怎么办？ | ✅ 已实现 | 幂等生产者 + ACKS=all + Outbox 持久化 + SynchronousDLQ Recoverer + DLT 死信队列 | 完整 |
| 6 | MQ 消息延迟消费了怎么办？ | ✅ 已实现 | Kafka Outbox 继电器 + MySQL 租约抢占 + 指数退避 + SENT 重查机制 + 对账重建 | 完整 |
| 7 | 数据库库存与 Redis 不一致怎么办？ | ✅ 已实现 | Lua 原子扣减 + INSERT IGNORE + 乐观锁 + 冲突补偿脚本 + 幂等校验 | 完整 |
| 8 | Redis 恢复后，丢失的数据怎么恢复？ | ✅ 已实现 | `SeckillVoucherStockInitializer` 启动恢复 + `Synchronizer` 元数据同步 | 完整 |

---

## 逐题详解

### 1. 流量突刺时，如何动态的限流？

**实现方案：双维度令牌桶限流（Lua 脚本原子执行）**

- **文件**：`SeckillRateLimitServiceImpl.java` + `lua/seckill_token_bucket.lua`
- **核心思路**：在 Redis Hash 中维护每个 IP 和每个用户的令牌桶状态（`tokens` + `lastMillis`），每次请求按时间流逝补充令牌，令牌不足则拒绝。
- **双重维度**：同时检查 IP 限流（防刷）和用户限流（防重复），两者都通过才放行。
- **动态配置**：通过 `SeckillRateLimitProperties` 配置 `ipWindowMillis`、`ipCapacity`、`userCapacity` 等参数，支持运行时调整。
- **用户等级乘数**：VIP 用户（`level >= vipMinLevel`）容量 × 2，高消费用户（`credits >= 1000`）容量 × 3，不叠加取最大值。
- **白名单机制**：支持 IP 白名单和用户白名单，绕过限流（用于测试账号）。
- **两个场景分别限流**：`ISSUE_SECKILL_ACCESS_TOKEN`（发令牌）和 `SECKILL_ORDER`（下单）各自有独立的限流配置。

**返回值定义**：
- `0` = 允许通过
- `10007` = IP 限流超出
- `10008` = 用户限流超出

**亮点**：比原始版的固定窗口/滑动窗口更精准，令牌桶更符合真实流量特征；双维度限流 + 用户等级差异化是加分项。

---

### 2. Redis 宕机了怎么办？

**实现方案：Redis Sentinel 高可用 + 应用层缓存降级 + 启动自动恢复**

整个方案分为三层：基础设施高可用、应用层降级、故障后自动恢复。

#### 第一层：Redis Sentinel 高可用（基础设施层）

项目部署了 1主2从 + 3哨兵节点的 Redis 集群（`application-redis-sentinel.yaml`）：
- 主节点（port 6379，AOF 每秒刷盘）+ 2 个从节点异步复制
- 3 个哨兵节点（port 26379，quorum=2，down-after 3000ms，failover-timeout 10000ms）
- Lettuce 客户端连接哨兵自动发现主节点，超时 2 秒，关闭超时 200ms
- Redisson 通过 `SentinelServersConfig` 自动感知主从切换，单例模式作为降级备选

主节点宕机后，哨兵集群在数秒内自动选举新主，Lettuce 客户端无感知切换，**秒杀请求不会中断**。

#### 第二层：应用层缓存降级

当 Sentinel 也未能及时恢复（极端场景）时，应用层有降级兜底：
- **限流环节**（`SeckillRateLimitServiceImpl`）：Lua 脚本执行失败时抛出自定义异常，用户看到"限流服务暂时不可用，请稍后重试"，不会穿透到数据库。
- **布隆过滤器**：`mightContain()` 在异常时 fail-open，不会误拦截合法请求。
- **店铺缓存**：`CacheCompatLock` 在 Redis 不可用时直接查 MySQL。
- **秒杀 Lua**：无法降级（原子扣减必须依赖 Redis），但 Sentinel 已将宕机窗口降至数秒。

#### 第三层：故障恢复后自动重建

应用重启或 Redis 恢复后，`SeckillVoucherStockInitializer` 自动修复：
1. 遍历 MySQL 所有秒杀券，调用 `setIfAbsent` 补建缺失的库存 Key（**只补不覆盖**）
2. 刷新活动元数据（beginTime/endTime/status），物理 TTL 覆盖活动结束时间
3. 布隆过滤器批量加载合法 voucherId，初始化完成后才对外服务

#### 为什么 Outbox 继电器不用 Redis 锁

Sentinel 故障转移期间仍有数秒命令阻塞窗口，如果 Outbox 继电器依赖 Redis 分布式锁，会导致投递停滞。因此使用 **MySQL 租约**（`UPDATE ... WHERE lease_until < NOW()`）替代，彻底消除对 Redis 的依赖，实现真正的独立可用。

#### 面试时的回答建议

> 当前项目部署了 Redis Sentinel 高可用集群（1主2从+3哨兵），主节点宕机后数秒内自动完成故障转移，Lettuce 客户端无感知切换。
>
> 同时还有三层兜底：
> 1. **应用层降级**：限流失败时 fail-fast 返回友好提示，布隆过滤器 fail-open，店铺缓存直接查 MySQL。
> 2. **启动自动恢复**：应用重启时 `SeckillVoucherStockInitializer` 用 `setIfAbsent` 从 MySQL 补建缺失库存 Key（只补不覆盖），元数据物理 TTL 覆盖活动结束时间。
> 3. **订单路径零 Redis 依赖**：Outbox 继电器使用 MySQL 租约替代 Redis 锁，即使 Redis 完全宕机，订单投递也能正常进行。
>
> 整个方案的核心前提是：Redis 只是缓存加速器，MySQL 是唯一权威数据源。Sentinel 负责减少宕机窗口，应用层负责容忍残留窗口，启动恢复负责最终修复。

---

### 3. Redis 数据丢失了怎么办？

**实现方案：启动恢复 + 元数据分层同步**

- **库存 Key 恢复**：`SeckillVoucherStockInitializer.run()` 在应用启动时遍历所有秒杀券，调用 `SeckillVoucherRedisSynchronizer.initializeVoucher()` 使用 `setIfAbsent` 补建缺失的库存 Key，**不会覆盖 Redis 中已有的实时库存**（只补不覆盖）。
- **布隆过滤器恢复**：启动时批量加载所有合法 voucherId，`initialize()` 完成后才允许拒绝请求，避免初始化期间误判。
- **元数据同步**：`SeckillVoucherRedisSynchronizer.synchronizeMetadata()` 将活动开始/结束时间、状态、逻辑过期时间写入 Hash，物理 TTL 覆盖活动结束时间 + 兜底缓冲时间（`staleGraceSeconds`），防止 Lua 脚本因 meta 过期而误拒绝请求。
- **新增/修改/删除三种路径**：
  - 新增：`synchronizeNewVoucher()` 写入初始库存 + 元数据
  - 修改：`synchronizeMetadata()` 只刷新元数据，不动库存
  - 删除：`deleteVoucher()` 删除 stock/meta/null 三个 Key + 失效本地缓存

---

### 4. MQ 宕机了怎么办？

**实现方案：Kafka 多副本 + Outbox 表双重保障**

- **Kafka 高可用配置**（`SeckillVoucherCacheKafkaConfig.java`）：
  - Topic 创建时指定 `replicas: 3`、`min.insync.replicas: 2`，保证至少 2 个副本存活才能写入。
  - `unclean.leader.election.enable: false`，防止数据丢失选举新 Leader。
- **幂等生产者**：`ENABLE_IDEMPOTENCE=true` + `ACKS=all`，确保生产者侧不丢消息。
- **Outbox 兜底**：即使 Kafka 全部宕机，缓存失效事件已持久化到 `tb_seckill_voucher_l1_invalidation_outbox` 表。
  - `SeckillVoucherLocalCacheInvalidationOutboxRelay` 定时轮询 PENDING 状态的事件，指数退避重试，最多 12 次后转入 FAILED 停车场，不丢失。
  - 分布式锁（Redisson）保证多实例只有一台实例处理，避免重复投递。

---

### 5. MQ 消息丢失了怎么办？

**实现方案：三层持久化保证**

| 层次 | 机制 | 作用 |
|------|------|------|
| 生产者侧 | 幂等 Producer + ACKS=all | Broker 写入成功才返回 |
| 消费侧 | MANUAL_IMMEDIATE 手动提交 | 业务处理完才提交 offset |
| 持久化侧 | Outbox 表 + DLT 死信队列 | 消息永久不丢，失败可人工介入 |

- **DLT 死信机制**：重试 3 次仍失败的消息，由 `SynchronousDeadLetterPublishingRecoverer` 同步发送到死信 Topic（`seckill-voucher-cache-invalidation.dlt`），**而不是异步发送**，避免 offset 提交后 DLT 发送失败导致双重丢失。
- **SynchronousDeadLetterPublishingRecoverer** 重写 `publish()` 使用 `.get(timeout)` 同步等待，发送失败时抛出异常阻止 offset 提交，由错误处理器重试。

---

### 6. MQ 消息延迟消费了怎么办？

**实现方案：Kafka Outbox 继电器 + MySQL 租约 + 多态投递重试**

> 秒杀订单消费链路已从 Redis Stream 升级为 Kafka Outbox 模式，彻底解耦对 Redis 的依赖。

**架构流程**

```
seckill.lua 原子扣减库存
        ↓
Redis ZSet Handoff（过渡缓冲区）
        ↓
Java 事务写 Outbox 表 (INSERT IGNORE)
        ↓
SeckillOrderOutboxRelay 定时轮询 PENDING 事件
        ↓
MySQL 租约抢占 → Kafka 发送
        ↓
SeckillOrderKafkaConsumer 创建订单 + 标记 COMPLETED
        ↓
营销后置（topBuyer、订阅、通知）
```

**核心组件**

| 组件 | 职责 |
|------|------|
| `SeckillOrderOutboxService` | 事务内写 Outbox 表，INSERT IGNORE 防并发重复写入 |
| `SeckillOrderOutboxRelay` | 定时扫描 PENDING/SENT 事件，MySQL 租约抢占，指数退避重试 |
| `SeckillOrderKafkaProducer` | 幂等生产者（ENABLE_IDEMPOTENCE + ACKS=all），按 voucherId 分片 |
| `SeckillOrderKafkaConsumer` | 创建订单、ACK、营销后置处理 |
| `SeckillOrderCompensationService` | DLT 死信补偿：Redis 预扣回滚 + 人工核对兜底 |
| `SeckillOrderHandoffService` | Redis ZSet 过渡层：Lua 成功但 Outbox 未落库的窗口期缓冲 |
| `SeckillOrderHandoffRecovery` | 启动/定时扫描 ZSet 过期成员，补写 Outbox 表 |
| `SeckillOrderReconciliationService` | 定时对账：以 MySQL Outbox 为权威，重建 Redis 库存投影 |

**MySQL 租约机制（关键创新）**

Outbox 继电器使用 MySQL 行级租约替代 Redis 分布式锁，实现多实例协调：

```java
// 抢租约：UPDATE ... WHERE relay_lease_until < NOW()（租约过期自动释放）
outboxMapper.claimRelay(event.getId(), relayOwner, leaseUntil);
// 发送成功：markSent
// 发送失败：scheduleRetry（指数退避：1s → 2s → 4s → ... → max 300s）
// SENT 重查：markSent 时设置 next_check_time，超时未 COMPLETED 则重新投递
```

对比旧方案的优势：
- **零 Redis 依赖**：即使 Redis 完全宕机，Outbox 表仍正常投递
- **租约自动过期**：实例宕机后租约到期自动释放，无需人工干预
- **SENT 重查机制**：发送成功但未收到消费确认的事件，会在时间窗口后重新投递，由消费端幂等兜底

**Outbox 五态机**

```
PENDING → SENT → COMPLETED
                  → COMPENSATED（DLT 死信补偿）
                  → MANUAL_REVIEW（补偿失败，人工介入）
```

**补偿脚本**（`seckill_compensate.lua`）

DLT 触发补偿时，使用 Lua 脚本原子删除 Redis 库存 Key 和已购用户集合，避免重复扣减：

```lua
redis.call('DEL', stockKey)   -- 删除库存 key，等对账重建
redis.call('SREM', orderKey, userId)
```

**对账重建脚本**（`seckill_rebuild.lua`）

定时对账时，以 MySQL Outbox 为权威数据源，重写 Redis 库存投影：

```lua
SET stockKey expectedStock
DEL orderKey
SADD orderKey userId1 userId2 ...  -- 从 Outbox + DB 重建已购集合
```

---

### 7. 数据库库存和 Redis 不一致怎么办？

**实现方案：Kafka Outbox 幂等 + 补偿 Lua + 定时对账重建**

| 场景 | 解决方案 |
|------|----------|
| 并发超卖 | `seckill.lua` 原子扣减 + INSERT IGNORE 幂等 + Handoff 过渡缓冲 |
| Outbox 写入失败 | Lua 在 ZSet 留下 Handoff 记录，`SeckillOrderHandoffRecovery` 恢复 |
| Kafka 消费失败 | DLT 死信 → `compensate()` 执行 `seckill_compensate.lua` 删除库存预留 |
| 重复消费 | Outbox 以 orderId 为唯一键，`markCompleted` 幂等更新 |
| Redis 库存漂移 | `SeckillOrderReconciliationService` 定时以 MySQL 为权威重建 Redis 投影 |

**关键设计**：MySQL Outbox 表是唯一权威数据源，Redis 库存是投影副本，对账脚本保证两者最终一致。

---

### 8. Redis 恢复后，丢失的数据要怎么恢复？

**实现方案：Handoff 恢复 + 对账重建双重保障**

- **Handoff 恢复**（`SeckillOrderHandoffRecovery`）：
  1. 扫描所有秒杀券的 Redis ZSet（`seckill:order:handoff:{voucherId}`）
  2. 找出 score 已过期的成员（Lua 扣减成功但 Outbox 未落库的窗口期）
  3. 解析 member（orderId|userId|autoIssued）写入 Outbox 表
  4. 成功后从 ZSet 移除

- **对账重建**（`SeckillOrderReconciliationService`）：
  1. 以 MySQL Outbox 表 + 订单表为权威数据
  2. 计算应保留库存 = DB stock - Outbox 未落单数 - Handoff 残留数
  3. 收集所有已购用户 ID（Outbox + DB 取并集）
  4. 执行 `seckill_rebuild.lua` 原子重建 stock key + order set
  5. 使用 Redis 租约（`setIfAbsent` + TTL）防止多实例同时重建

---

## 与 hmdp-plushmdp-plus 参考实现对比

| 功能模块 | 当前项目 | 参考实现（Plus） |
|----------|----------|------------------|
| 限流算法 | Lua 令牌桶（完整） | 同左 |
| 访问令牌 | Lua 签发/消费脚本（完整） | 同左 |
| 秒杀缓存 | Caffeine L1 + Redis L2 + Bloom Filter | L1 + L2 + Bloom（结构相同） |
| Outbox 模式 | Kafka Outbox（商品 + 秒杀券 + 秒杀订单三路径） | 单一路径（商品） |
| 订单消费 | Kafka + MySQL 租约 Outbox 继电器（完整解耦） | Redis Stream（有 Redis 依赖） |
| 库存恢复 | Handoff 过渡 + 对账重建双保障 | setIfAbsent 只补不覆盖 |
| 取消订单补偿 | CANCEL_ROLLBACK_SCRIPT 事务回滚补偿 | 同左 |
| Redis 高可用 | Redis Sentinel 1主2从+3哨兵自动故障转移 | 无（参考实现也是单机） |
| 哨兵/集群 | 已部署（compose.yaml + application-redis-sentinel.yaml） | 无 |

**结论**：当前项目已经吸收了 Plus 版本的核心改进，在限流、访问令牌、缓存架构、Outbox 模式、库存恢复等方面实现完整，部分实现（如双维度限流、用户等级乘数、SynchronousDLQ Recoverer）甚至超出了参考版本的复杂度。更关键的升级是将秒杀订单消费链路从 Redis Stream 迁移至 Kafka Outbox 模式，彻底消除订单路径对 Redis 的依赖，并引入 MySQL 租约替代分布式锁、SENT 重查机制、对账重建等高级可靠性设计。

---

## 简历亮点建议

基于以上分析，建议在简历中按以下结构描述项目：

1. **秒杀缓存架构**：基于 Caffeine L1 + Redis L2 的两层缓存，配合 Bloom Filter 防穿透、Lua 逻辑过期防击穿，QPS 提升显著。
2. **分布式限流**：基于 Redis Lua 令牌桶算法实现 IP + 用户双维度限流，支持 VIP 用户容量乘数动态配置；集成背压反馈，Outbox 积压自动收紧入口。
3. **秒杀访问令牌**：前置令牌校验机制，Lua 脚本原子签发/消费，防刷防重放。
4. **缓存一致性**：事务型 Outbox 模式 + Kafka 持久化投递，保证缓存失效事件不丢失；Outbox 继电器指数退避重试 + 租约抢占防多实例重复。
5. **秒杀订单 Kafka 管道**（核心亮点）：将原 Redis Stream 订单消费链路升级为 Kafka Outbox 模式——秒杀请求在 MySQL 事务内写入 Outbox 表，Kafka 继电器组件轮询投递，彻底消除订单链路对 Redis 的依赖；支持 DLT 死信补偿（Redis 预扣回滚）和 MySQL 租约抢占，多实例安全。
6. **动态压力反馈限流**：定时采样 Outbox 积压量，按 NORMAL/WARNING/CRITICAL 三级动态调整入口令牌桶容量，避免下游越堵入口仍按固定速率放行。
7. **Redis 对账重建**：定时任务以 MySQL Outbox 表为权威数据源，重建 Redis 库存投影和已购用户集合，解决库存漂移问题；使用 Redis 租约防止多实例同时重建。
8. **幂等保障**：生产者 ACKS=all + 幂等开关、消费者 MANUAL_IMMEDIATE 提交、Outbox INSERT IGNORE + eventId 幂等键、补偿脚本原子操作。
