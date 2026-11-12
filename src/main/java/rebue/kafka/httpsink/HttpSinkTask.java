package rebue.kafka.httpsink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP Sink 的 task。
 *
 * <p>职责很窄：把记录渲染成请求体 → 交给 {@link HttpSender} → 按结果决定抛不抛异常。
 * offset 提交完全交给框架（默认 {@code preCommit} 返回当前位点），所以「抛出异常 = 不提交」
 * 这条语义必须靠这里守住：任何失败都不能被吞成日志。
 */
public class HttpSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(HttpSinkTask.class);

    private HttpSinkConfig config;
    private HttpSender sender;

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(final Map<String, String> props) {
        this.config = new HttpSinkConfig(props);
        this.sender = HttpSender.create(config);
        log.info("HTTP Sink 启动: url={} method={} version={} batch={} envelope={} max.retries={}",
                config.urlTemplate(), config.getString(HttpSinkConfig.HTTP_METHOD), Version.VERSION,
                config.getBoolean(HttpSinkConfig.BATCH_ENABLED),
                config.getBoolean(HttpSinkConfig.BATCH_ENVELOPE),
                config.maxRetries());
        // 这两种行为会让失败变成"看起来成功"，offset 照常提交，等于丢数据。启动时把话说清，
        // 免得日后在死信页找不到记录时才发现
        final String behavior = config.getString(HttpSinkConfig.BEHAVIOR_ON_ERROR);
        if (!"fail".equals(behavior)) {
            log.warn("behavior.on.error={}：投递失败不会被抛给框架，记录会被跳过且 offset 照常提交，"
                    + "死信（DLQ）里不会有这些记录", behavior);
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        if (records == null || records.isEmpty()) {
            return;
        }
        if (config.getBoolean(HttpSinkConfig.BATCH_ENABLED)) {
            putBatch(records);
            return;
        }
        for (final SinkRecord record : records) {
            putOne(record);
        }
    }

    private void putOne(final SinkRecord record) {
        if (record.value() == null) {
            // tombstone：上游的删除标记，没有消息体可投递。跳过不算失败，框架会正常提交 offset
            log.debug("跳过 tombstone: topic={} partition={} offset={}",
                    record.topic(), record.kafkaPartition(), record.kafkaOffset());
            return;
        }
        final String body = Json.toBody(record.value(),
                config.getString(HttpSinkConfig.VALUE_FORMAT),
                config.getString(HttpSinkConfig.VALUE_BYTES_ENCODING));
        final HttpSender.Result result = sender.send(config.urlFor(record.topic()), record, body);
        handle(result, record);
    }

    /**
     * 批量投递：把若干条 value 拼成一个 JSON 数组。
     *
     * <p>整批共享一次响应：要么全成功、要么全失败。跨记录的失败粒度没有了，所以默认关闭。
     */
    private void putBatch(final Collection<SinkRecord> records) {
        final int maxSize = config.getInt(HttpSinkConfig.BATCH_MAX_SIZE);
        final boolean envelope = config.getBoolean(HttpSinkConfig.BATCH_ENVELOPE);
        final List<SinkRecord> batch = new ArrayList<>(Math.min(records.size(), maxSize));
        final List<String> bodies = new ArrayList<>(Math.min(records.size(), maxSize));

        for (final SinkRecord record : records) {
            if (record.value() == null) {
                continue;
            }
            final String body = Json.toBody(record.value(),
                    config.getString(HttpSinkConfig.VALUE_FORMAT),
                    config.getString(HttpSinkConfig.VALUE_BYTES_ENCODING));
            batch.add(record);
            bodies.add(envelope ? envelope(record, body) : body);
            if (bodies.size() >= maxSize) {
                sendBatch(batch, bodies);
                batch.clear();
                bodies.clear();
            }
        }
        if (!bodies.isEmpty()) {
            sendBatch(batch, bodies);
        }
    }

    private void sendBatch(final List<SinkRecord> batch, final List<String> bodies) {
        final String body = Json.toBatchBody(bodies);
        // 一批跨多条记录，请求头里的源坐标会失真，因此不传 record：坐标改由信封（envelope）
        // 或下游自己的业务主键承担
        final HttpSender.Result result = sender.send(config.urlFor(batch.get(0).topic()), null, body);
        if (result.success()) {
            log.debug("批量投递成功: 记录数={} 首条 offset={}", batch.size(), batch.get(0).kafkaOffset());
            return;
        }
        // 失败时把批次里第一条的坐标带进异常，至少能定位到区间起点
        handle(result, batch.get(0));
    }

    /** 信封：把源坐标和值一起发给下游，批量模式下也能做幂等 */
    private static String envelope(final SinkRecord record, final String body) {
        final StringBuilder sb = new StringBuilder(body.length() + 128);
        sb.append("{\"metadata\":{\"topic\":");
        appendQuoted(sb, record.topic());
        sb.append(",\"partition\":")
                .append(record.kafkaPartition() == null ? "null" : record.kafkaPartition().toString());
        sb.append(",\"offset\":").append(record.kafkaOffset());
        sb.append(",\"timestamp\":").append(record.timestamp() == null ? "null" : record.timestamp());
        sb.append("},\"value\":").append(body).append('}');
        return sb.toString();
    }

    private static void appendQuoted(final StringBuilder sb, final String text) {
        sb.append('"');
        for (int i = 0; i < text.length(); i++) {
            final char ch = text.charAt(i);
            if (ch == '"' || ch == '\\') {
                sb.append('\\');
            }
            sb.append(ch);
        }
        sb.append('"');
    }

    /**
     * 统一处理投递结果。
     *
     * <p>{@code fail} 之外的两种行为都会吞掉失败，仅在明确接受丢数据时才该用。
     */
    private void handle(final HttpSender.Result result, final SinkRecord record) {
        if (result.success()) {
            log.trace("投递成功: status={} topic={} offset={}",
                    result.status, record == null ? "-" : record.topic(),
                    record == null ? "-" : record.kafkaOffset());
            return;
        }
        switch (config.getString(HttpSinkConfig.BEHAVIOR_ON_ERROR)) {
            case "ignore":
                log.debug("投递失败已忽略: {}", result.describe());
                return;
            case "log":
                log.error("投递失败已跳过（behavior.on.error=log）: {}", result.describe());
                return;
            case "fail":
            default:
                throw result.toException(record);
        }
    }

    @Override
    public void stop() {
        // Java 17 的 HttpClient 没有 close()；工作线程是共享的 daemon 池，无需在此回收
        this.sender = null;
        this.config = null;
    }
}
