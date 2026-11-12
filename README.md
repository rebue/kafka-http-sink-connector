# kafka-http-sink-connector

把 Kafka 记录投递到 HTTP 端点的 Kafka Connect Sink 插件。零第三方运行时依赖，只用
`connect-api` + `slf4j`（都由 worker 提供）。

> **不兼容 wheel-kafka 3.5.x**：类名 `rebue.kafka.httpsink.HttpSinkConnector`，配置键统一到
> `http.*` 命名空间。尚未正式上线，版本号仍是 1.0.0。迁移见[从 3.5.x 迁移](#从-35x-迁移)。

## 为什么重写

老实现（`wheel-kafka/http-sink-connector` 3.5.x）对任何 HTTP 状态码都只记一行日志、不抛异常。
Connect 因此认为投递成功并提交 offset：**4xx/5xx 的记录静默消失，`errors.tolerance` 与死信
配置全部失效**。反过来，一次下游抖动（`IOException`）被包成 `RuntimeException`，在
`errors.tolerance=all` 下直接进死信、不重试 —— 语义正好是反的。

重写的就是这件事，顺带修掉几个会在生产放大的设计缺陷：

| 问题 | 3.5.x | 本版 |
| --- | --- | --- |
| 失败语义 | 只记日志，offset 照常提交 | 2xx/4xx/5xx 三分类，映射到 Connect 的异常类型 |
| 重试 | 无 | 指数退避 + 抖动 + 硬时间预算，尊重 `Retry-After` |
| 死信粒度 | 无 | `ErrantRecordReporter` 逐条上报，任务继续跑 |
| 时间预算 | 无（可能睡穿 poll 间隔触发 rebalance） | 每次 `put()` 有硬预算，超了就抛可重试异常 |
| 序列化 | `value.toString()`（`Struct{...}` 不是 JSON） | schema 感知的 JSON 写入器，含逻辑类型 |
| 多 topic 路由 | 所有记录打一个地址 | `http.url` 支持 `${topic}`，按 topic 路由 |
| 批量 | 无 | 数组 + 可选信封，失败可拆单定位坏记录 |
| 多 task | `taskConfigs` 只返回 1 份 | 按 `tasks.max` 分发 |
| 密钥 | 启动时 `log.info(props)` 打印全部配置 | 只打印脱敏摘要 |

## 错误语义

整条死信链路成立与否，全看这张表。插件把 HTTP 响应翻译成 Connect 认识的三类：

| 情况 | 判定 | Connect 侧行为 |
| --- | --- | --- |
| 2xx | 成功 | 提交 offset |
| 408 / 425 / 429 / 5xx | 可重试 | 插件内退避重试；用尽后抛 `RetriableException`，框架按 `errors.retry.*` 再来一轮，**不提交 offset** |
| 3xx | 不可重试 | 按 `behavior.on.error` 处理（不跟随重定向：POST 被 301/302 改写为 GET 会丢掉请求语义） |
| 400 / 404 / 409 / 415 / 422 … | 不可重试 | 按 `behavior.on.error` 处理；默认 `report` 逐条进死信 |
| 401 / 403 | 不可重试 | 按 `behavior.on.error` 处理，异常信息直接点明"检查 http.auth.*" |
| 连接失败 / 读超时 / TLS 握手失败 | 可重试 | 同 5xx |
| 序列化失败 | 不可重试 | 记录级问题，按 `behavior.on.error` 处理 |

`behavior.on.error` 决定记录级失败怎么落地：

- `report`（默认）：交给 `ErrantRecordReporter` **逐条**进死信，任务继续跑、offset 正常提交。
  需要同时配 `errors.tolerance=all` 与 `errors.deadletterqueue.topic.name`；没有可用 reporter
  时退回抛异常，不会悄悄丢数据。
- `fail`：抛回 `put()`。**别和 `errors.tolerance=all` 搭着用**——KIP-610 把记录级错误的
  容错责任交给 task 自己上报后，Connect 不再用 `errors.tolerance` 兜 `put()` 抛出的异常；
  实际表现是一条 4xx 让整个 task 退出、offset 不提交、重启后反复重放同一条坏记录。
- `skip`：直接丢弃，会丢数据，只在明确接受时才用。

> 这一默认值从 `fail` 改为 `report`：平台已经建了死信链路，一条坏记录不该停掉整条管道。
> 集群里若还混跑旧版插件（默认值是 `fail`），建议在连接器配置里**显式写**
> `"behavior.on.error": "report"`，免得换 jar 前后行为不一致。

> 逐条上报比"整批抛异常"精确：Connect 的 `put()` 是批量的，一条记录抛异常会连带整批走同一
> 分支；`report` 只把真正失败的那几条送进死信。

## 投递保证

**至少一次**。重试、任务重启、rebalance 都会让同一条记录被投递多次，所以下游需要幂等。插件
为此给出两组坐标：

- 单条模式：`X-Kafka-Topic` / `X-Kafka-Partition` / `X-Kafka-Offset` / `X-Kafka-Timestamp` /
  `X-Kafka-Key`，以及可直接去重的 `X-Idempotency-Key: {topic}-{partition}-{offset}`；
- 批量模式：请求头里的坐标会失真，打开 `http.batch.envelope=true` 让每条记录带
  `{"metadata":{...},"value":...}`。

单分区内顺序由"同步发送、串行投递"保证；需要**跨分区**有序就把 `tasks.max` 设为 1。

## 配置

| 键 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- |
| `http.url` | string | **必填** | 目标地址，仅 http/https。支持 `${topic}` 占位符。不允许带用户名密码 |
| `http.method` | string | `POST` | `POST` / `PUT` / `PATCH` |
| `http.protocol` | string | `HTTP_1_1` | `HTTP_1_1` / `HTTP_2`。HTTP_2 对明文要求服务端支持，升级失败会连不上 |
| `http.content.type` | string | `application/json` | Content-Type |
| `http.user.agent` | string | `kafka-connect-http-sink/1.0.0` | User-Agent |
| `http.compression` | string | `none` | `none` / `gzip` |
| `http.connect.timeout.ms` | int | `5000` | 建连超时 |
| `http.request.timeout.ms` | int | `30000` | 单次请求超时（会被剩余预算裁剪） |
| `http.delivery.budget.ms` | int | `60000` | **单次 `put()` 的总预算**，0 表示不限。必须明显小于 `consumer.max.poll.interval.ms` |
| `http.retry.max` | int | `8` | 插件内最多重试几次（不含首次） |
| `http.retry.backoff.ms` | int | `500` | 首次退避，之后指数增长 |
| `http.retry.backoff.max.ms` | int | `30000` | 退避上限，同时约束 `Retry-After` |
| `http.retry.jitter.ms` | int | `250` | 退避抖动上限 |
| `http.headers.*` | string | — | 自定义请求头，如 `http.headers.X-Tenant=acme`。`Host`/`Authorization` 等保留头会被忽略 |
| `http.metadata.headers.enabled` | bool | `true` | 是否发送源坐标头与幂等键 |
| `http.header.forward` | list | `[]` | 透传哪些 Kafka 记录头（前缀 `x-kafka-header-`），`*` 表示全部 |
| `http.payload.format` | string | `auto` | `auto` 按值类型分派；`json` 一律序列化；`text` 一律纯文本 |
| `http.payload.bytes.encoding` | string | `base64` | byte[] 的编码：`base64` / `utf8` |
| `http.batch.enabled` | bool | `false` | 多条记录拼成一个 JSON 数组 |
| `http.batch.max.records` | int | `100` | 单批最大条数 |
| `http.batch.envelope` | bool | `false` | 每条包成 `{metadata, value}` |
| `http.batch.isolate.failures` | bool | `true` | 批量失败时拆单重投以定位坏记录（会产生重复投递） |
| `http.auth.type` | string | `none` | `none` / `basic` / `bearer` |
| `http.auth.username` / `http.auth.password` | string | — | basic 认证；`http.auth.type=basic` 时必填 |
| `http.auth.token` | password | — | bearer 认证；`http.auth.type=bearer` 时必填 |
| `http.tls.truststore.path` / `.password` / `.type` | string | — / — / `JKS` | 校验自签证书的服务端 |
| `http.tls.insecure` | bool | `false` | 跳过证书校验，**仅限联调** |
| `behavior.on.error` | string | `report` | `fail` / `report` / `skip`；配了 DLQ 就用 `report` |
| `http.stats.interval.ms` | int | `60000` | 投递汇总日志间隔，0 关闭 |

配置错误在 `POST/PUT /connectors` 时就会返回，并被 `PUT /connector-plugins/{class}/config/validate`
定位到具体键（跨字段问题也一样），不必等 task 反复 FAILED 才发现。

### 配置示例

```json
{
  "name": "orders-http-sink",
  "config": {
    "connector.class": "rebue.kafka.httpsink.HttpSinkConnector",
    "tasks.max": "1",
    "topics": "orders",
    "http.url": "http://downstream:8080/sync/${topic}",
    "http.retry.max": "5",
    "http.delivery.budget.ms": "30000",
    "behavior.on.error": "report",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "connect-dlq",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.retry.timeout": "300000"
  }
}
```

## 构建与部署

```bash
mvn package                      # 产物 target/kafka-http-sink-connector-<version>.jar，约 64KB
```

1. 把 jar 直接覆盖到 worker 的 `plugin.path` 下原来的插件目录
   （例如 `/opt/kafka/plugins/http-sink-connector/`），目录里只留这一个 jar；
2. 重启 worker。Connect 只在启动时扫描插件目录，**没有热加载**；
3. `GET /connector-plugins` 确认出现 `rebue.kafka.httpsink.HttpSinkConnector`。

同一个目录里只放这一版：新 jar 与 3.5.x 的 jar 定义了同一个类名
（`rebue.wheel.kafka.httpsink.HttpSinkConnector` 在 3.5.x 里也存在），同时存在时
加载哪个由扫描顺序决定，不确定。

## 从 3.5.x 迁移

三个变化必须一起改，否则连接器起不来或者配置静默失效：

| 项 | 3.5.x | 本版 |
| --- | --- | --- |
| 类名 | `rebue.wheel.kafka.httpsink.HttpSinkConnector` | `rebue.kafka.httpsink.HttpSinkConnector` |
| 地址 | `http.api.url` | `http.url` |
| 重试 | `max.retries` / `retry.backoff.ms` | `http.retry.max` / `http.retry.backoff.ms` |
| 版本 | `http.version` | `http.protocol` |
| 报文 | `value.format` / `value.bytes.encoding` | `http.payload.format` / `http.payload.bytes.encoding` |

`deploy/migrate-config.py` 通过 Connect REST 原地改写：

```bash
# 先看会改成什么（默认只打印差异）
./deploy/migrate-config.py --connect http://kafka01:8083 --connector dtw-to-dtwrs-dtw-region-sink

# 确认后写入
./deploy/migrate-config.py --connect http://kafka01:8083 --connector dtw-to-dtwrs-dtw-region-sink --apply
```

顺序不能颠倒：**先**把 jar 替换好并重启 worker，**再**改连接器配置。反过来的话，
PUT 会因为集群上找不到新类而返回 400。

## 运维注意

- **死信量会从 0 变成真实值**。切过来之后 4xx 会第一次真正进入死信队列，先确认死信 topic
  的分区数与副本数扛得住（单 broker 环境需要把 `errors.deadletterqueue.topic.replication.factor`
  调到 1，否则建 topic 会失败）。
- **预算与 poll 间隔**：`http.delivery.budget.ms` 必须小于 `consumer.max.poll.interval.ms`。
  超了会触发 rebalance，一次下游变慢就被放大成重复投递。
- **批量 + 拆单**会带来重复投递，下游要么幂等，要么关掉 `http.batch.isolate.failures`。
- 汇总日志（`http.stats.interval.ms`）给出的是"自上次汇总以来"的增量：投递条数、字节数、
  尝试次数、死信条数、预算耗尽次数。排查时先看它，再去看逐条日志。
- `http.tls.insecure=true` 会在启动时告警；生产开启等于放弃传输层身份认证。

## 已知边界

- 不支持 OAUTHBEARER / mTLS 客户端证书；需要双向 TLS 的场景请前置网关。
- 不做请求体分片：单条记录多大就发多大。
- 批量模式下没有"部分成功"协议：整批共享一个状态码，靠拆单兜底。
- 死信写入失败会转成可重试异常（记录留在原位），不会静默丢弃，但会让整个 task 回退重试。
