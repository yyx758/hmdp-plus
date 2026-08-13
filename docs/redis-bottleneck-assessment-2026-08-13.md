# Redis 与秒杀链路瓶颈评估（2026-08-13）

## 结论

当前一主二从 Sentinel Redis 不是秒杀链路的首要瓶颈，不建议现在仅为提升吞吐迁移 Redis Cluster。

- Redis 6.2，AOF `everysec`，两副本在线，无连接拒绝、淘汰和 AOF 延迟。
- 128 B `SET`、无 pipeline：1/10/50/100 连接约为 9.5k/65.7k/75.0k/74.0k ops/s。
- 128 B `SET`、50 连接、pipeline=16：约 462.7k ops/s（仅表示批量吞吐上界）。
- 单热点券、50 连接、无 pipeline 的成功分支等价 Lua（检查元数据/库存/重复下单，执行 `INCRBY + SADD + ZADD`）：18,772 req/s，P50 2.319 ms。该轮主线程 CPU 增量 5.214 秒/约 5.33 秒，已经接近单主单核上限。
- 项目已有完整链路稳定结果为 8,674.9 QPS、P99 48 ms；当时 Redis 平均 130,627 ops/s、约 0.54 核。入口吞吐只有本轮单热点成功 Lua 上限的约 46%。

因此当前优先瓶颈在应用入口和 Redis 之后的可靠消息/数据库链路，而不是 Redis 裸写能力。

## 当前代码中的主要限制

1. 请求线程同步写 MySQL Outbox：Redis Lua 成功后，接口线程执行 Outbox `INSERT IGNORE`，再 `SELECT` 回读，最后删除 Redis Handoff。入口吞吐与 MySQL 单事务延迟直接耦合。
2. Outbox Relay 串行：每 100 ms 查询最多 100 条，但在 `for` 循环中逐条 claim、同步等待 Kafka `send().get()`、逐条更新 `SENT`。单实例无法把批次转化为并发或批量吞吐。
3. Kafka 消费端逐条事务：虽然有 6 分区/6 消费线程，但每条消息单独插订单、扣热点库存行、更新 Outbox，再手动 ACK。当前 `createVoucherOrders(List)` 的批量能力没有被 Kafka listener 使用。
4. 数据库池只有 16，且 MySQL 使用 `innodb_flush_log_at_trx_commit=1`、`sync_binlog=1`。这是正确的可靠性配置，但逐条事务会放大 fsync 和连接占用成本。
5. 单实例入口已观察到 300/400 并发时连接建立失败，而 200 并发稳定；应先验证监听队列、压测机端口和 Tomcat/Nginx，再扩应用实例。

## 优化优先级

### P0：恢复真正的批处理

- Outbox Relay：批量 claim，Kafka 异步发送（有界 in-flight），批量更新发送状态；不要在调度线程内逐条 `get()`。
- Kafka listener：启用 batch listener，按分区/券聚合 50–200 条，调用现有 `createVoucherOrders(List)`；一个批次一次 `INSERT IGNORE`、每券一次库存扣减、批量完成 Outbox，提交后再 ACK。
- 保留二分隔离/重试/DLT，避免单条坏消息拖垮整批。
- 压测验收同时观察：入口 QPS/P99、Outbox backlog/最老事件年龄、Kafka lag、订单落库 QPS、连接池 active/pending、MySQL redo/fsync/行锁。

### P1：解耦入口与 MySQL（需要可靠性设计评审）

当前 Redis Handoff 已保存 `orderId|userId|retry`，可考虑让接口在 Lua 成功后返回 `PROCESSING`，由恢复任务把 Handoff 批量落 Outbox；这样入口不再同步等待 MySQL。代价是受理语义从“Outbox 已提交”变为“Redis 已持久化且等待转存”，必须明确 AOF everysec 最多约 1 秒风险窗口，并通过 `WAIT`、更强 AOF 策略或业务补偿选择一致性等级。不要直接删除现有可靠性闭环。

### P1：扩应用实例与入口容量

在独立压测机上重测 200/300/400/600 并发，采集 Nginx active/waiting、accepts、Tomcat current/busy/max、listen overflow、JVM CPU/GC。确认单实例饱和后，通过 Nginx 横向扩 Java；Redis 和 MySQL仍是共享资源，扩容时必须看总体利用率和 backlog。

### P2：何时才迁移 Redis Cluster

满足任一条件再启动 3 主 3 从 Cluster PoC：

- 生产同等 Lua 的 Redis 主线程持续超过 70%，或 P99 明显恶化；
- 估算峰值成功预扣超过单主安全容量。以本机 18.8k/s 为实验上限、预留 40% 余量，单热点券安全目标约 11k/s；
- 多个券并行且总负载可按券分散到多个主分片；
- 内存容量或故障域要求已经超过单主 Sentinel。

Cluster 对“同一张热点券”基本不能线性扩展：库存、购买集合、活动元数据、资格令牌、Handoff 必须在同一个 hash slot 中，仍由单个主节点串行执行。只有多券/多业务 key 能均匀分片时，3 主理论聚合吞吐才可能接近单主的 2–3 倍，实际需扣除倾斜、迁槽和故障余量。

当前 key 还没有统一 hash tag：`seckill:stock:<id>`、`seckill:order:<id>`、`seckill:meta:<id>` 与 `seckill:order:handoff:{<id>}` 在 Cluster 中不能直接由同一 Lua 跨 key 执行。迁移前必须统一为例如：

```text
seckill:{voucherId}:stock
seckill:{voucherId}:orders
seckill:{voucherId}:meta
seckill:{voucherId}:access:<userId>
seckill:{voucherId}:handoff
seckill:{voucherId}:recovery
```

并扫描所有 Lua、pipeline、事务、批量删除、Redisson 锁和运维脚本的跨 slot 行为。

## 本轮测试边界

- 测试在本机 Docker/WSL2 上执行，不能替代生产同规格主机和独立压测机的容量验收。
- 等价 Lua 使用隔离券 ID `987650001`，完成后已删除测试元数据、库存、购买集合、Handoff 与恢复 key。
- 没有对当前真实下单接口写入压测数据：本地 18081 已被 IDE Java 进程占用，登录测试 token 返回 401；为避免污染业务表，本轮没有伪造用户、订单或库存。
- Kafka 当前为空载、6 分区/6 消费者，lag 为 0；MySQL 空载时 16 个连接、1 个 running，无行锁等待。空载指标只证明环境健康，不能证明下游峰值容量。
