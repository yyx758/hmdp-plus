# hmdp-plus

基于黑马点评课程项目演进的高并发实践版本。项目保留商户、博客、关注、签到、优惠券和秒杀等核心业务，并重点补充多级缓存、可靠缓存失效、秒杀异步落库、运行时数据同步与容器化环境。

## 核心增强

### 缓存穿透与缓存击穿组合链路

秒杀券活动元数据采用以下读取顺序：

```text
Caffeine L1
    ↓ 未命中
Redis 共享布隆过滤器
    ↓ 可能存在
Redis L2 活动元数据
    ↓ 未命中
Redis 空值缓存
    ↓ 未命中
Redisson 分布式重建锁
    ↓
Double-Check
    ↓
MySQL
```

- **Caffeine L1**：缓存券 ID、商户 ID、开始时间、结束时间和状态，降低热点请求的 Redis 网络开销。
- **Redis Bloom Filter**：启动时加载合法秒杀券 ID，新增券时动态写入；初始化异常时 fail-open，避免错误拒绝合法请求。
- **空值缓存**：数据库不存在的数据写入短 TTL 空值，并增加随机抖动，拦截重复非法请求。
- **Double-Check-Locking**：使用 Redisson 分布式锁保证多实例下只有一个线程重建；获取锁后再次检查 L1、Redis 和空值缓存。
- **逻辑过期**：Redis 保留旧值并记录 `logicalExpireAt`。过期请求先返回旧值，再由有界线程池异步重建，避免热点瞬间击穿数据库。
- **启动预热**：应用启动时初始化 Bloom Filter，补建缺失的 Redis 库存并预热活动元数据与本机缓存。

> Caffeine 只缓存活动元数据，**不缓存实时库存**。库存始终由 Redis Lua 原子校验和扣减；Lua 同时最终校验活动状态、开始时间和结束时间，防止旧的本地数据放行无效订单。

核心实现：

- `SeckillVoucherCacheService`：组合读取、空值缓存、DCL 与异步重建。
- `SeckillVoucherLocalCache`：Caffeine 一级缓存。
- `SeckillVoucherBloomFilter`：Redisson 共享布隆过滤器。
- `SeckillVoucherRedisSynchronizer`：新增/修改/启动时同步 Redis 元数据并预热 L1。
- `SeckillVoucherStockInitializer`：启动预热和缺失库存恢复。
- `seckill.lua`：活动状态、时间、一人一单与库存的最终原子裁决。

### 其他高并发能力

- 商户查询使用 Caffeine + Redis 两级缓存。
- 商户缓存修改使用 Kafka + Transactional Outbox 可靠广播跨 JVM 失效消息。
- 秒杀请求通过 Redis Lua 原子校验，使用 Redis Stream 消费者组异步落库。
- 秒杀消费支持批量写入、失败拆批、Pending 重试、死信队列与订单状态查询。
- 新增和修改秒杀券时同步 Redis 活动元数据；普通修改不会覆盖正在变化的实时库存。
- Docker Compose 提供 MySQL、3 个 Redis、Kafka 与应用的一键本地环境。

## 技术栈

- Java 8 / Spring Boot 2.3
- MyBatis-Plus / MySQL 5.7 / Flyway
- Redis / Redisson / Redis Stream / Lua
- Caffeine
- Kafka
- Docker Compose

## 快速启动

### Docker Compose

首次启动前准备一个本地图片目录：

```powershell
$env:HMDP_FRONTEND_IMAGE_DIR = "$PWD/data/imgs"
New-Item -ItemType Directory -Force $env:HMDP_FRONTEND_IMAGE_DIR
docker compose up -d --build
docker compose ps
```

默认端口：

| 服务 | 地址 |
| --- | --- |
| 应用 | `http://127.0.0.1:18081` |
| MySQL | `127.0.0.1:3308` |
| Redis | `127.0.0.1:6380` |
| Redis 2 | `127.0.0.1:6381` |
| Redis 3 | `127.0.0.1:6382` |
| Kafka | `127.0.0.1:9092` |

示例接口：

```text
GET http://127.0.0.1:18081/shop-type/list
POST http://127.0.0.1:18081/voucher-order/seckill/{voucherId}
```

### 本地运行

本地默认连接 MySQL `3308`、Redis `6380/6381/6382` 和 Kafka `9092`。启动基础设施后运行：

```powershell
mvn spring-boot:run
```

关键缓存配置位于 `src/main/resources/application.yaml` 的：

```yaml
hmdp:
  cache:
    seckill-voucher:
```

可通过环境变量控制 L1 容量、L1 TTL、逻辑 TTL、空值 TTL、Bloom 参数以及异步重建线程池。

## 测试

运行全部测试：

```powershell
mvn test
```

运行缓存穿透与缓存击穿相关测试：

```powershell
mvn "-Dtest=SeckillVoucherLocalCacheTest,SeckillVoucherBloomFilterTest,SeckillVoucherCacheServiceTest,SeckillVoucherRedisSynchronizerTest,SeckillVoucherStockInitializerTest,VoucherServiceImplTest,VoucherOrderSeckillTimeTest" test
```

测试覆盖 L1 命中、Bloom 拒绝、Redis 命中回填、空值短路、分布式锁 DCL、锁竞争保护、数据库空结果和逻辑过期异步重建。

## 生产环境说明

- Compose 中 Kafka 单副本配置只用于本地开发；生产环境应使用至少 3 个 Broker、`acks=all` 和合适的 `min.insync.replicas`。
- 示例密码只用于本地环境，部署前必须通过环境变量替换。
- Caffeine 是 JVM 私有缓存，不能作为库存或订单资格的最终事实源。
- Redis Lua 是秒杀入口的最终一致性校验点，MySQL 约束与消费端幂等作为持久化兜底。
