# 秒杀令牌桶 A/B 压测

该测试使用同一个登录用户持续请求申请秒杀 Access Token 的接口，模拟脚本或恶意客户端绕过前端进行高并发请求。

默认负载：100 个并发线程、2 秒升压、持续 20 秒。A 组关闭令牌桶，B 组开启令牌桶，其他代码、数据和请求参数保持一致。

JMeter 运行示例：

```powershell
jmeter -n `
  -t load-tests/jmeter/seckill-rate-limit-ab.jmx `
  -Jthreads=100 `
  -Jramp_seconds=2 `
  -Jduration_seconds=20 `
  -Jauth_token=jmeter-rate-limit-test `
  -Jvoucher_id=987654321 `
  -l result.jtl
```

测试登录态使用 Redis Hash `login:token:jmeter-rate-limit-test`。测试完成后应删除该临时 Key。测试券 ID 使用不存在的 `987654321`，不会扣减库存或创建订单。

这里选择“申请 Access Token”接口，是为了只切换令牌桶开关：关闭时所有请求都会进入 Token 签发 Lua；开启时超过配额的请求在入口返回 HTTP 429。这样不会把库存扣减、一人一单或消息队列等变量混入结果。

## 混合流量 A/B/C 测试

`seckill-rate-limit-mixed.jmx` 同时运行两组流量：

- 攻击流量：默认固定为 3,000 请求/秒，持续 30 秒，使用单独的用户、券 ID、转发 IP 和测试客户端标识。
- 正常流量：10 个独立用户，每个用户约每秒请求一次，使用另一张测试券。

测试时只使用 `LEGIT_OK` 样本衡量正常用户延迟。攻击请求会根据实际去向标记为 `ATTACK_ALLOWED`、`ATTACK_APP_REJECTED` 或 `ATTACK_NGINX_REJECTED`。

```powershell
jmeter -n `
  -t load-tests/jmeter/seckill-rate-limit-mixed.jmx `
  -Jattack_threads=100 `
  -Jattack_qps=3000 `
  -Jattack_exact_limit=90000 `
  -Jduration_seconds=30 `
  -Jlegit_threads=10 `
  -Jlegit_delay_millis=1000 `
  -Jport=18081 `
  -l mixed-result.jtl
```

A 组关闭应用限流；B 组开启 Redis 令牌桶并直接访问应用端口；C 组保持 Redis 令牌桶开启，通过 `nginx-rate-limit-test.conf` 的独立 18082 端口访问。测试 Nginx 仅用于本地压测，不替换 18080 的前端代理。

正式 18080 入口对比时可增加以下参数：

```powershell
-Jhost=127.0.0.1 `
-Jport=18080 `
-Jpath_prefix=/api `
-Jattack_source_ip=127.0.0.2 `
-Jlegit_source_ip=127.0.0.3
```

攻击流量和正常流量必须使用不同源 IP，否则 Nginx 按 `$binary_remote_addr` 限流时，两组流量会共用同一只网关桶。`X-Forwarded-For` 仅用于验证 Nginx 会覆盖伪造值，不能用它模拟不同的真实客户端 IP。
