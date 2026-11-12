# kafka-http-sink-connector

Kafka Connect 的 HTTP Sink 连接器，把 Kafka 记录 POST 到 HTTP 端点。

这一版是为替换 `rebue.wheel.kafka.httpsink.HttpSinkConnector` 重写的。老实现能跑通「把消息发出去」，
但在**失败语义**上和 Kafka Connect 的容错机制对不上，导致死信（DLQ）整条链路形同虚设。

## 老实现的五个问题

| 问题 | 后果 |
| --- | --- |
| `put()` 对 4xx/5xx 只打日志、不抛异常 | Connect 认为投递成功并提交 offset，坏数据静默丢失，DLQ 里没有 |
| `IOException` 被包成 `RuntimeException` | `errors.tolerance=all` 下直接写 DLQ、**不重试**；一次网络抖动就生成一条死信 |
| `value.toString()` 当请求体 | `schemas.enable=true` 时是 `Struct{id=1001}`，不是 JSON，下游无法解析 |
| `taskConfigs()` 只返回 1 份配置 | `tasks.max` 填多少都只有一个 task |
| `log.info(props)` 打印全部配置 | `http.auth.password` 明文进日志 |

## 快速开始

```json
{
  "name": "http-sink-orders",
  "config": {
    "connector.class": "rebue.kafka.httpsink.HttpSinkConnector",
    "topics": "dbz.orders",
    "tasks.max": "1",
    "http.url": "http://magic-api:9999/sync/${topic}",
    "max.retries": "10",
    "retry.backoff.ms": "3000",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```

```bash
curl -X PUT -H 'Content-Type: application/json' \
  http://127.0.0.1:8083/connectors/http-sink-orders/config \
  -d @config.json
```

## 错误语义（这是重点）

| 下游响应 | 插件行为 | 框架行为 |
| --- | --- | --- |
| 2xx | 成功 | 提交 offset |
| 408 / 429 / 5xx / 连接超时 / 读超时 | 先按 `max.retries` 重试，仍失败抛 `RetriableException` | 按 `errors.retry.*` 再重试，超时后写 DLQ |
| 其它 4xx（400/401/403/404/409…） | 直接抛 `DataException`，不重试 | `errors.tolerance=all` 下写 DLQ |
| 3xx | 视为配置错误，不重试（不跟随重定向） | 同上 |

被判为「成功」的记录会提交 offset；被判为「失败」的记录**不会**提交，进 DLQ 后由人工重投。
所以下游必须用**非 2xx** 表达业务失败；用 HTTP 200 + `{"result":-2}` 的话，任何连接器都接不住。

## 幂等：一定会重复投递

重试、task 重启、rebalance 都会让同一条记录被 POST 多次，这是 at-least-once 的固有语义。

默认每个请求带这些头，下游可据此去重：

```
X-Kafka-Topic: dbz.orders
X-Kafka-Partition: 3
X-Kafka-Offset: 1042
X-Kafka-Timestamp: 1762912345678
X-Kafka-Key: 1001
```

关掉：`http.metadata.headers.enable=false`。

## 配置

### 目标与请求

| 键 | 默认 | 说明 |
| --- | --- | --- |
| `http.url` | 必填 | 支持 `${topic}` 占位符；只支持这一种占位符 |
| `http.api.url` | — | 已废弃，`http.url` 为空时回落到它 |
| `http.method` | `POST` | `POST` / `PUT` / `PATCH` |
| `http.version` | `HTTP_1_1` | `HTTP_1_1` / `HTTP_2`；明文 HTTP/2 依赖服务端支持 |
| `http.content.type` | `application/json` | 老配置 `http.header.content-type` 仍生效 |
| `http.user.agent` | `kafka-connect-http-sink/1.0.0` | |
| `http.compression` | `none` | `gzip` 需要下游支持 `Content-Encoding` |
| `http.header.<名字>` | — | 自定义请求头，如 `http.header.X-Tenant=acme` |

### 超时与重试

| 键 | 默认 | 说明 |
| --- | --- | --- |
| `http.connect.timeout.ms` | `5000` | 建连超时 |
| `http.request.timeout.ms` | `30000` | 单次请求读超时 |
| `http.retry.total.timeout.ms` | `120000` | 单条记录的重试总预算，**必须小于 `max.poll.interval.ms`** |
| `max.retries` | `10` | 与 Confluent HTTP Sink 同名，dbz 注入即可生效 |
| `retry.backoff.ms` | `3000` | 首次退避，之后按 2 的幂增长 |
| `retry.backoff.max.ms` | `30000` | 退避上限 |
| `retry.jitter.ms` | `1000` | 抖动上限，避免多个 task 同时重试打崩下游 |

`Retry-After` 响应头会被尊重（仅支持秒数形式）。

### 行为

| 键 | 默认 | 说明 |
| --- | --- | --- |
| `behavior.on.error` | `fail` | `fail` 抛给框架（进 DLQ）；`log`/`ignore` 会**丢数据**，仅限明确接受时使用 |
| `value.format` | `auto` | `auto` / `string` / `json` / `bytes` |
| `value.bytes.encoding` | `base64` | `base64` / `utf8` |
| `http.metadata.headers.enable` | `true` | 是否带 `X-Kafka-*` 坐标头 |
| `http.header.forward` | `false` | 是否把记录 headers 透传成 `x-kafka-header-*` |
| `http.batch.enabled` | `false` | 合成 JSON 数组一次发送 |
| `http.batch.max.size` | `100` | 单请求最多条数 |
| `http.batch.envelope` | `false` | 元素包成 `{"metadata":{...},"value":...}`，批量下也能幂等 |

`value.format=auto` 的规则：字符串原样输出（配合 `JsonConverter` + `schemas.enable=false`）、
`Struct`/`Map` 转 JSON、逻辑类型按语义渲染（`Decimal` → 数字、`Timestamp` → ISO-8601、`bytes` → BASE64）。

### 认证与 TLS

| 键 | 默认 | 说明 |
| --- | --- | --- |
| `http.auth.type` | `none` | `none` / `basic` / `bearer` |
| `http.auth.username` / `http.auth.password` | — | basic；密码是 `PASSWORD` 类型，Connect REST 返回时打码，不落日志 |
| `http.auth.token` | — | bearer |
| `http.tls.truststore.path` / `.password` / `.type` | — / — / `JKS` | 自签证书用；路径必须在每个 worker 本地存在 |
| `http.tls.insecure.skip.verify` | `false` | **仅联调**，启动时会 warn |

## 迁移

1. 把 `target/kafka-http-sink-connector-1.0.0.jar` 放进每个 worker 的 `plugin.path`；
2. **删掉老的 `http-sink-connector` jar**（同一个类名出现在两个 jar 里时，加载哪个不确定）；
3. 重启 worker（Connect 没有热加载插件的接口）；
4. 已有连接器可以不改配置：`connector.class` 为 `rebue.wheel.kafka.httpsink.HttpSinkConnector`
   时由兼容类接管；新连接器建议用 `rebue.kafka.httpsink.HttpSinkConnector`。

`GET /connector-plugins` 里应能看到两个同名不同包的条目（迁移期正常）。

## 与 dbz 配合

- dbz 会注入 `errors.tolerance=all`、`errors.deadletterqueue.*`、`max.retries`、`retry.backoff.ms`——
  这些键本插件全部认识，注入即生效（老实现只认 4 个键，注入等于没注）。
- 死信记录由 Connect 写入 DLQ topic，`__connect.errors.*` 头里带源 topic/partition/offset、
  异常类名与堆栈，dbz 据此在死信页展示与重投。
- 重投是写回**源 topic**，会重新走一遍转换器与 SMT；若该 topic 被多条 sink 订阅，它们都会再收到一次。

## 构建

```bash
mvn package          # 跑测试 + 打 shaded jar，产物无第三方依赖
```

Java 17+。插件只依赖 `connect-api` / `kafka-clients`（provided，worker 自带），
不打包 Jackson，避免与 worker 的 classpath 冲突。

## 边界

- 不跟随重定向：POST 被 3xx 重定向时语义会丢，配置里直接写最终地址。
- 批量模式下整批共享一次响应，没有单条失败粒度；需要逐条 DLQ 时不要开批量。
- 多 task 只保证单分区内有序；需要全局有序时 `tasks.max=1`。
- 没有实现 `preCommit` 的额外位点管理：失败即不提交，语义由框架保证。
