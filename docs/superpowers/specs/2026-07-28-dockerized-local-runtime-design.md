# 黑马点评 Docker 本地运行设计

## 目标

使用 Docker Compose 启动 Java 应用、MySQL 和 Redis，同时保证所有新增容器、网络和数据卷只属于当前项目，不影响其他项目。

## 运行架构

- Compose 使用固定项目名 `hm-dianping-dev`。
- `app`、`mysql`、`redis` 三个服务共享当前项目的专属网络。
- 仅将应用的 `8081` 端口映射到宿主机。
- MySQL 和 Redis 不映射宿主机端口，避免端口冲突和被其他项目误用。
- MySQL 与 Redis 使用当前项目的命名卷持久化数据。
- MySQL 首次创建数据卷时自动执行 `src/main/resources/db/hmdp.sql`。

## 配置

- `application.yaml` 中数据库和 Redis 的地址、端口、用户名及密码支持环境变量覆盖，并保留现有值作为本机运行默认值。
- `RedissonConfig` 使用 Spring Redis 配置，不再包含硬编码地址或密码。
- Compose 通过环境变量让应用访问服务名 `mysql` 和 `redis`。

## 构建

- 新增多阶段 `Dockerfile`：Maven 阶段构建 Spring Boot JAR，JRE 阶段运行应用。
- 新增 `.dockerignore`，排除 Git 元数据、IDE 文件和本地构建产物。
- Compose 等待 MySQL 和 Redis 健康后再启动应用。

## 隔离与安全边界

- 不使用 `docker system prune`、全局停止、全局删除或其他跨项目操作。
- 不声明通用的固定容器名，避免与其他 Compose 项目冲突。
- 停止或清理时只带当前 Compose 文件和项目名操作。
- 不删除数据卷，除非用户以后明确要求重置本项目数据。
- 保留用户现有的 `application.yaml` 未提交修改，并在实现时只添加必要的环境变量占位能力。

## 验证

1. 校验 Compose 配置能够正常解析。
2. 构建并启动三个服务。
3. 检查 MySQL、Redis 和应用容器健康或运行状态。
4. 验证 `hmdp` 数据库已导入表。
5. 请求一个无需登录的 HTTP 接口，确认应用可从宿主机访问。

