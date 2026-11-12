package rebue.kafka.httpsink;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rebue.kafka.httpsink.delivery.DeliveryResult;
import rebue.kafka.httpsink.delivery.DeliveryStats;
import rebue.kafka.httpsink.delivery.HttpTransport;
import rebue.kafka.httpsink.delivery.RetryPolicy;
import rebue.kafka.httpsink.json.JsonBody;
import rebue.kafka.httpsink.route.UrlTemplate;
import rebue.kafka.httpsink.support.RequestHeaders;

/**
 * HTTP Sink 的 task。
 *
 * <p>职责只有三件：把记录编成请求、按失败语义决定"抛不抛"，以及保证 {@code put()} 不会
 * 超过时间预算。
 *
 * <p>为什么失败处理这么绕：Connect 的 sink 是**批量**调用 {@code put(records)} 的，一条记录
 * 抛异常会连带整批进入相同的处理分支。所以：
 * <ul>
 *   <li>可重试的失败（5xx/超时）一律抛出，让框架回到上一个已提交位点重来 —— 宁可重复，
 *       不能丢；</li>
 *   <li>记录级失败（4xx/序列化不了）优先交给 {@link ErrantRecordReporter} **逐条**送进死信，
 *       任务继续跑，offset 正常提交。没有 reporter（未配 DLQ）时才退回抛 {@link DataException}；</li>
 *   <li>批量请求失败时先拆成单条重投，把坏记录从批次里摘出来，别让它连坐整批。</li>
 * </ul>
 */
public class HttpSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(HttpSinkTask.class);

    /** 等待死信写入完成的上限：死信写失败必须被看见，不能当成"处理完了" */
    private static final long DLQ_WAIT_MS = 30_000;

    private HttpSinkConfig config;
    private UrlTemplate urls;
    private RequestHeaders headers;
    private HttpTransport transport;
    private RetryPolicy retry;
    private JsonBody json;
    private DeliveryStats stats;
    private ErrantRecordReporter reporter;
    private long statsIntervalMs;
    private long nextStatsAtNanos;

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(final Map<String, String> props) {
        this.config = new HttpSinkConfig(props);
        for (final String warning : config.warnings()) {
            log.warn("配置提醒: {}", warning);
        }
        this.urls = config.urlTemplate();
        this.headers = RequestHeaders.of(config);
        this.transport = new HttpTransport(config);
        this.json = new JsonBody(config.bytesAsBase64());
        this.retry = new RetryPolicy(config.getInt(HttpSinkConfig.HTTP_RETRY_MAX),
                config.getInt(HttpSinkConfig.HTTP_REQUEST_TIMEOUT_MS),
                config.getInt(HttpSinkConfig.HTTP_RETRY_BACKOFF_MS),
                config.getInt(HttpSinkConfig.HTTP_RETRY_BACKOFF_MAX_MS),
                config.getInt(HttpSinkConfig.HTTP_RETRY_JITTER_MS));
        this.stats = new DeliveryStats();
        this.statsIntervalMs = config.getInt(HttpSinkConfig.HTTP_STATS_INTERVAL_MS);
        this.nextStatsAtNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(statsIntervalMs);
        this.reporter = reporter();

        log.info("HTTP Sink 已启动 版本={} {}", Version.VERSION, config.summary());
        if (config.behavior() == HttpSinkConfig.Behavior.SKIP) {
            log.warn("behavior.on.error=skip：失败的记录会被直接丢弃，且不会进入死信队列");
        }
        if (config.behavior() == HttpSinkConfig.Behavior.REPORT) {
            if (reporter == null) {
                log.warn("behavior.on.error=report 但当前没有可用的 ErrantRecordReporter"
                        + "（需要 errors.tolerance=all 且配置了死信 topic），将退化为抛异常");
            } else {
                // 默认路径，用 info：每个 task 启一次，不该当成告警刷屏
                log.info("behavior.on.error=report：坏记录逐条进死信，任务继续消费");
            }
        }
        if (config.behavior() == HttpSinkConfig.Behavior.FAIL) {
            log.warn("behavior.on.error=fail：4xx 抛回 put() 会让任务退出，"
                    + "errors.tolerance=all 不会兜住（KIP-610），建议改成 report");
        }
    }

    /**
     * 取死信上报器。
     *
     * <p>{@code errantRecordReporter()} 在未配置死信时会返回 null，个别实现还会直接抛
     * {@link UnsupportedOperationException}；两者都当作"不可用"，不能让它把 task 打死。
     */
    private ErrantRecordReporter reporter() {
        if (context == null) {
            return null;
        }
        try {
            return context.errantRecordReporter();
        } catch (final RuntimeException e) {
            log.debug("取 ErrantRecordReporter 失败，按不可用处理: {}", e.toString());
            return null;
        }
    }

    @Override
    public void open(final Collection<TopicPartition> partitions) {
        if (partitions != null && !partitions.isEmpty()) {
            log.info("分到 {} 个分区: {}", partitions.size(), partitions);
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        if (records == null || records.isEmpty()) {
            return;
        }
        final long deadline = deadline();
        int handled = 0;
        for (final Unit unit : plan(records)) {
            if (outOfBudget(deadline)) {
                stats.budgetExhausted();
                // 抛而不是继续：剩余记录没有提交 offset，交给框架下一轮重投。
                // 已经发出去的那部分会重复投递 —— 这是 at-least-once 的固有代价，
                // 请求头里的幂等键就是给下游去重用的。
                throw new RetriableException("投递预算(" + config.getInt(HttpSinkConfig.HTTP_DELIVERY_BUDGET_MS)
                        + "ms)已耗尽：本轮处理了 " + handled + "/" + records.size()
                        + " 条，剩余记录将随下一轮重投");
            }
            deliver(unit, deadline);
            handled += unit.records().size();
        }
        maybeLogStats();
    }

    /* ------------------------------------------------------------------ 规划 */

    /**
     * 把一批记录切成投递单元。
     *
     * <p>关掉批量时一条一个单元；打开批量时按 **URL + topic** 分组再切片 —— 一个 task 可能同时
     * 订阅多个 topic，用 {@code ${topic}} 路由时它们的目标地址并不相同，混成一批会把数据发错地方。
     * 分组用 {@link LinkedHashMap} 保持首次出现顺序，避免同一分区的记录被反复重排。
     */
    private List<Unit> plan(final Collection<SinkRecord> records) {
        final List<Unit> units = new ArrayList<>(records.size());
        if (!config.getBoolean(HttpSinkConfig.HTTP_BATCH_ENABLED)) {
            for (final SinkRecord record : records) {
                if (record.value() == null) {
                    // tombstone：上游的删除标记，没有可投递的报文
                    continue;
                }
                units.add(new Unit(urls.resolve(record.topic()), List.of(record), false));
            }
            return units;
        }

        final int maxRecords = config.getInt(HttpSinkConfig.HTTP_BATCH_MAX_RECORDS);
        final Map<String, List<SinkRecord>> groups = new LinkedHashMap<>();
        for (final SinkRecord record : records) {
            if (record.value() == null) {
                continue;
            }
            final String key = urls.resolve(record.topic()) + "\u0000" + record.topic();
            groups.computeIfAbsent(key, ignored -> new ArrayList<>()).add(record);
        }
        for (final List<SinkRecord> group : groups.values()) {
            for (int from = 0; from < group.size(); from += maxRecords) {
                final List<SinkRecord> slice = group.subList(from, Math.min(from + maxRecords, group.size()));
                units.add(new Unit(urls.resolve(slice.get(0).topic()), List.copyOf(slice), true));
            }
        }
        return units;
    }

    /* ------------------------------------------------------------------ 投递 */

    private void deliver(final Unit unit, final long deadline) {
        // 按配置而不是按单元大小判断：批量模式下只来了 1 条记录也必须发数组，
        // 否则下游时而收到对象、时而收到数组，契约就崩了
        final String body;
        try {
            body = unit.batched()
                    ? batchBody(unit.records())
                    : json.render(unit.records().get(0).value(), config.payloadFormat());
        } catch (final RuntimeException e) {
            // 序列化不了是记录级问题，和 HTTP 无关
            handleRecordFailure(unit, new DataException(
                    "记录无法渲染成请求体: " + e.getMessage(), e), deadline);
            return;
        }

        final HttpTransport.Payload payload = HttpTransport.encode(body, config.gzip());
        final Map<String, String> requestHeaders = unit.batched()
                ? headers.forBatch()
                : headers.forRecord(unit.records().get(0));

        final DeliveryResult result = retry.execute(
                timeoutMs -> transport.send(payload, unit.uri(), requestHeaders, timeoutMs), deadline);

        if (result.delivered()) {
            stats.delivered(unit.records().size(), payload.bytes().length, result.attempts());
            if (log.isTraceEnabled()) {
                log.trace("投递成功 {} 条 -> {} ({})", unit.records().size(), unit.uri(), result.describe());
            }
            maybeLogStats();
            return;
        }
        if (result.kind() == DeliveryResult.Kind.POISON) {
            handleRecordFailure(unit, result.toException(unit.uri().toString()), deadline);
            return;
        }
        // 可重试且已用尽：绝不吞。抛出后 offset 不提交，框架按 errors.retry.* 再来一轮
        log.warn("投递失败（可重试）{} 条 -> {}: {}", unit.records().size(), unit.uri(), result.describe());
        throw result.toException(unit.uri().toString());
    }

    private String batchBody(final List<SinkRecord> records) {
        final List<String> bodies = new ArrayList<>(records.size());
        final boolean envelope = config.getBoolean(HttpSinkConfig.HTTP_BATCH_ENVELOPE);
        for (final SinkRecord record : records) {
            final String body = json.render(record.value(), config.payloadFormat());
            bodies.add(envelope ? envelope(record, body) : body);
        }
        return json.array(bodies);
    }

    /** 信封：批量模式下也能让下游按坐标去重 */
    private static String envelope(final SinkRecord record, final String body) {
        final StringBuilder sb = new StringBuilder(body.length() + 128);
        sb.append("{\"metadata\":{\"topic\":");
        JsonBody.quote(record.topic(), sb);
        sb.append(",\"partition\":").append(record.kafkaPartition());
        sb.append(",\"offset\":").append(record.kafkaOffset());
        sb.append(",\"timestamp\":").append(record.timestamp());
        sb.append("},\"value\":").append(body).append('}');
        return sb.toString();
    }

    /* ------------------------------------------------------------------ 记录级失败 */

    /**
     * 处理一条（或一批）注定失败的记录。
     *
     * <p>批量时先拆单重投：坏记录通常只有一条，让整批陪葬既不准也难查。拆单会带来重复投递，
     * 所以 {@code http.batch.isolate.failures} 可以关掉。
     */
    private void handleRecordFailure(final Unit unit, final RuntimeException cause, final long deadline) {
        if (unit.records().size() > 1
                && config.getBoolean(HttpSinkConfig.HTTP_BATCH_ISOLATE)
                && config.behavior() != HttpSinkConfig.Behavior.FAIL) {
            log.warn("批量投递失败，拆成单条重投以定位坏记录: {}", cause.getMessage());
            for (final SinkRecord record : unit.records()) {
                deliver(new Unit(unit.uri(), List.of(record), false), deadline);
            }
            return;
        }
        switch (config.behavior()) {
            case SKIP -> {
                stats.skipped(unit.records().size());
                log.error("记录级失败已按 behavior.on.error=skip 丢弃（不会进入死信）: {}",
                        cause.getMessage());
            }
            case REPORT -> report(unit, cause);
            case FAIL -> throw cause;
        }
    }

    private void report(final Unit unit, final RuntimeException cause) {
        if (reporter == null) {
            // 没有死信可用时退回抛异常：由框架的 errors.tolerance 决定去死信还是让任务失败，
            // 无论如何都不是"悄悄丢掉"
            throw cause;
        }
        for (final SinkRecord record : unit.records()) {
            final Future<Void> future;
            try {
                future = reporter.report(record, cause);
            } catch (final RuntimeException e) {
                throw new RetriableException("写入死信队列失败（" + record.topic() + "）", e);
            }
            awaitReport(future, record);
        }
        stats.errant(unit.records().size());
        log.error("记录已写入死信队列 {} 条（{}）: {}", unit.records().size(),
                unit.records().get(0).topic(), cause.getMessage());
    }

    /**
     * 等死信真正写进去。
     *
     * <p>{@code report} 是异步的，不等待就等于"任务以为处理完了，其实死信没写成"。这里用有界
     * 等待，超时或失败转成可重试异常 —— 让记录留在原位重来，而不是凭空消失。
     */
    private static void awaitReport(final Future<Void> future, final SinkRecord record) {
        try {
            future.get(DLQ_WAIT_MS, TimeUnit.MILLISECONDS);
        } catch (final ExecutionException e) {
            throw new RetriableException("写入死信队列失败（" + record.topic() + "/"
                    + record.kafkaPartition() + "@" + record.kafkaOffset() + "）", e.getCause());
        } catch (final TimeoutException e) {
            throw new RetriableException("写入死信队列超时（" + DLQ_WAIT_MS + "ms）", e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetriableException("等待死信写入时被中断", e);
        }
    }

    /* ------------------------------------------------------------------ 生命周期 */

    @Override
    public void close(final Collection<TopicPartition> partitions) {
        if (partitions != null && !partitions.isEmpty()) {
            log.info("释放 {} 个分区: {}", partitions.size(), partitions);
        }
    }

    @Override
    public void stop() {
        final String summary = stats == null ? null : stats.drain();
        if (summary != null) {
            log.info("HTTP Sink 停止前汇总: {}", summary);
        }
        this.config = null;
        this.urls = null;
        this.headers = null;
        this.transport = null;
        this.retry = null;
        this.json = null;
        this.stats = null;
        this.reporter = null;
    }

    /* ------------------------------------------------------------------ 预算与统计 */

    private long deadline() {
        final int budgetMs = config.getInt(HttpSinkConfig.HTTP_DELIVERY_BUDGET_MS);
        return budgetMs <= 0 ? RetryPolicy.NO_DEADLINE : System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(budgetMs);
    }

    private static boolean outOfBudget(final long deadline) {
        return deadline != RetryPolicy.NO_DEADLINE && System.nanoTime() >= deadline;
    }

    private void maybeLogStats() {
        if (statsIntervalMs <= 0 || System.nanoTime() < nextStatsAtNanos) {
            return;
        }
        nextStatsAtNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(statsIntervalMs);
        final String summary = stats.drain();
        if (summary != null) {
            log.info("HTTP Sink 汇总: {}", summary);
        }
    }

    /**
     * 一次投递单元。
     *
     * @param batched 是否按批量协议发送（数组 + 信封）。拆单重投时必须为 false，
     *                否则又会拼成一条只有一个元素的数组发出去
     */
    private record Unit(URI uri, List<SinkRecord> records, boolean batched) {
    }
}
