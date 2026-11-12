package rebue.kafka.httpsink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rebue.kafka.httpsink.delivery.HttpTransport;
import rebue.kafka.httpsink.support.RequestHeaders;

/**
 * 端到端投递语义。
 *
 * <p>这些用例覆盖的正是老实现出错的地方：失败被吞成日志、状态码不分、批量失败连坐整批、
 * 重试睡穿 poll 间隔。
 */
class HttpSinkTaskTest {

    private TestServer server;
    private HttpSinkTask task;

    @BeforeEach
    void setUp() throws IOException {
        server = TestServer.start();
    }

    @AfterEach
    void tearDown() {
        if (task != null) {
            task.stop();
        }
        server.close();
    }

    private void start(final FakeContext context, final String... overrides) {
        task = new HttpSinkTask();
        task.initialize(context);
        task.start(Configs.map(withUrl(overrides)));
    }

    /** 默认把 http.url 指向本地测试端点；用例可用 HttpSinkConfig.HTTP_URL 覆盖 */
    private String[] withUrl(final String... overrides) {
        final List<String> merged = new ArrayList<>();
        boolean urlOverridden = false;
        for (int i = 0; i < overrides.length; i += 2) {
            if (HttpSinkConfig.HTTP_URL.equals(overrides[i])) {
                urlOverridden = true;
            }
            merged.add(overrides[i]);
            merged.add(overrides[i + 1]);
        }
        if (!urlOverridden) {
            merged.add(HttpSinkConfig.HTTP_URL);
            merged.add(server.url("/sync"));
        }
        return merged.toArray(String[]::new);
    }

    private static SinkRecord record(final long offset, final String value) {
        return new SinkRecord("orders", 0, Schema.STRING_SCHEMA, "k" + offset,
                Schema.STRING_SCHEMA, value, offset);
    }

    /* ------------------------------------------------------------------ 成功路径 */

    @Test
    void deliversRecordWithIdempotencyKey() {
        start(new FakeContext());
        task.put(List.of(record(7, "{\"id\":7}")));

        final TestServer.Exchange exchange = server.only();
        assertEquals("POST", exchange.method());
        assertEquals("/sync", exchange.path());
        assertEquals("{\"id\":7}", exchange.body());
        assertEquals("orders-0-7", exchange.header(RequestHeaders.IDEMPOTENCY_KEY.toLowerCase()));
        assertEquals("application/json", exchange.header("content-type"));
    }

    /** tombstone 是上游的删除标记，没有报文可投，跳过不算失败 */
    @Test
    void skipsTombstones() {
        start(new FakeContext());
        task.put(List.of(record(1, null)));
        assertEquals(0, server.count());
    }

    /* ------------------------------------------------------------------ 失败语义 */

    @Test
    void clientErrorSurfacesAsDataException() {
        server.alwaysRespond(400).respondBody("{\"error\":\"bad payload\"}");
        start(new FakeContext(), HttpSinkConfig.BEHAVIOR_ON_ERROR, "fail");

        final DataException error = assertThrows(DataException.class,
                () -> task.put(List.of(record(1, "{}"))));
        assertTrue(error.getMessage().contains("400"));
        assertTrue(error.getMessage().contains("bad payload"));
        assertEquals(1, server.count(), "4xx 不应重试");
    }

    @Test
    void serverErrorIsRetriedThenSucceeds() {
        server.scriptResponses(503, 503);
        start(new FakeContext(),
                HttpSinkConfig.HTTP_RETRY_MAX, "3",
                HttpSinkConfig.HTTP_RETRY_BACKOFF_MS, "0",
                HttpSinkConfig.HTTP_RETRY_JITTER_MS, "0");
        task.put(List.of(record(1, "{}")));

        assertEquals(3, server.count());
    }

    @Test
    void exhaustedRetriesRaiseRetriableException() {
        server.alwaysRespond(500);
        start(new FakeContext(),
                HttpSinkConfig.HTTP_RETRY_MAX, "2",
                HttpSinkConfig.HTTP_RETRY_BACKOFF_MS, "0",
                HttpSinkConfig.HTTP_RETRY_JITTER_MS, "0");

        final RetriableException error = assertThrows(RetriableException.class,
                () -> task.put(List.of(record(1, "{}"))));
        assertTrue(error.getMessage().contains("500"));
        assertEquals(3, server.count(), "首次 + 2 次重试");
    }

    /** 3xx 不能跟随：POST 被 301/302 改写成 GET 会把请求语义整个丢掉 */
    @Test
    void redirectIsTreatedAsFailure() {
        server.redirectTo("http://127.0.0.1:9/elsewhere");
        start(new FakeContext(), HttpSinkConfig.BEHAVIOR_ON_ERROR, "fail");

        final DataException error = assertThrows(DataException.class,
                () -> task.put(List.of(record(1, "{}"))));
        assertTrue(error.getMessage().contains("重定向"), error.getMessage());
        assertEquals(1, server.count());
    }

    /**
     * 预算耗尽必须抛可重试异常，且不能真的睡满退避。
     *
     * <p>这是 rebalance 的源头：put() 阻塞超过 consumer.max.poll.interval.ms，协调者就认为
     * 这个 task 死了，把分区转给别人，于是同一条记录被投递两次。
     */
    @Test
    void budgetExhaustionDoesNotBlockPut() {
        server.delay(300);
        start(new FakeContext(),
                HttpSinkConfig.HTTP_DELIVERY_BUDGET_MS, "80",
                HttpSinkConfig.HTTP_REQUEST_TIMEOUT_MS, "5000",
                HttpSinkConfig.HTTP_RETRY_BACKOFF_MS, "5000",
                HttpSinkConfig.HTTP_RETRY_JITTER_MS, "0");

        final long startedAt = System.nanoTime();
        assertThrows(RetriableException.class, () -> task.put(List.of(record(1, "{}"), record(2, "{}"))));
        final long elapsedMs = (System.nanoTime() - startedAt) / 1_000_000;
        assertTrue(elapsedMs < 2_000, "put() 阻塞过久: " + elapsedMs + "ms");
    }

    /* ------------------------------------------------------------------ 死信 */

    /** 逐条上报比"整批抛异常"精确：只有坏记录进死信，任务继续跑 */
    @Test
    void reportBehaviorSendsPoisonRecordToDlq() {
        server.alwaysRespond(422).respondBody("{\"error\":\"invalid\"}");
        final FakeContext.RecordingReporter reporter = new FakeContext.RecordingReporter();
        start(new FakeContext(reporter), HttpSinkConfig.BEHAVIOR_ON_ERROR, "report");

        task.put(List.of(record(1, "{}")));

        assertEquals(1, reporter.records().size());
        assertEquals(1L, reporter.records().get(0).kafkaOffset());
        assertTrue(reporter.errors().get(0).getMessage().contains("422"));
    }

    /** 不写 behavior.on.error 时也必须逐条进死信，这才是改默认值的意义 */
    @Test
    void defaultBehaviorReportsPoisonRecordToDlq() {
        server.alwaysRespond(400);
        final FakeContext.RecordingReporter reporter = new FakeContext.RecordingReporter();
        start(new FakeContext(reporter));

        // 不抛异常 = 任务继续跑、offset 能正常提交
        task.put(List.of(record(1, "{}")));

        assertEquals(1, reporter.records().size());
        assertEquals(1L, reporter.records().get(0).kafkaOffset());
    }

    @Test
    void skipBehaviorDropsRecord() {
        server.alwaysRespond(400);
        final FakeContext.RecordingReporter reporter = new FakeContext.RecordingReporter();
        start(new FakeContext(reporter), HttpSinkConfig.BEHAVIOR_ON_ERROR, "skip");

        task.put(List.of(record(1, "{}")));

        assertTrue(reporter.records().isEmpty());
        assertEquals(1, server.count());
    }

    /** 死信写不进去时不能假装处理完了：转成可重试异常，记录留在原位 */
    @Test
    void dlqWriteFailureBecomesRetriable() {
        server.alwaysRespond(400);
        final FakeContext.RecordingReporter reporter = new FakeContext.RecordingReporter();
        reporter.failNextReport();
        start(new FakeContext(reporter), HttpSinkConfig.BEHAVIOR_ON_ERROR, "report");

        assertThrows(RetriableException.class, () -> task.put(List.of(record(1, "{}"))));
    }

    /** 没有 reporter（未配 DLQ）时退回抛异常，由 errors.tolerance 决定去向 */
    @Test
    void reportWithoutReporterFallsBackToException() {
        server.alwaysRespond(400);
        start(new FakeContext(), HttpSinkConfig.BEHAVIOR_ON_ERROR, "report");
        assertThrows(DataException.class, () -> task.put(List.of(record(1, "{}"))));
    }

    /* ------------------------------------------------------------------ 批量 */

    @Test
    void batchSendsOneArrayRequest() {
        start(new FakeContext(),
                HttpSinkConfig.HTTP_BATCH_ENABLED, "true",
                HttpSinkConfig.HTTP_BATCH_MAX_RECORDS, "10");
        task.put(List.of(record(1, "{\"a\":1}"), record(2, "{\"a\":2}")));

        assertEquals(1, server.count());
        assertEquals("[{\"a\":1},{\"a\":2}]", server.only().body());
        assertNull(server.only().header(RequestHeaders.IDEMPOTENCY_KEY.toLowerCase()),
                "批量请求不带单条坐标");
    }

    @Test
    void batchEnvelopeCarriesCoordinates() {
        start(new FakeContext(),
                HttpSinkConfig.HTTP_BATCH_ENABLED, "true",
                HttpSinkConfig.HTTP_BATCH_ENVELOPE, "true");
        task.put(List.of(record(5, "{\"a\":1}")));

        assertEquals("[{\"metadata\":{\"topic\":\"orders\",\"partition\":0,\"offset\":5,\"timestamp\":null},"
                + "\"value\":{\"a\":1}}]", server.only().body());
    }

    /**
     * 批量失败后拆单重投：坏记录被摘出来，好记录照常投递。
     *
     * <p>这里让端点"只在收到数组时失败"，模拟"整批被拒但每条单独发都能过"的场景。
     */
    @Test
    void batchIsolatesFailuresPerRecord() {
        // 只让第一次（整批）请求失败，拆单后的单条请求默认 200
        server.alwaysRespond(200).scriptResponses(400);
        start(new FakeContext(),
                HttpSinkConfig.HTTP_BATCH_ENABLED, "true",
                HttpSinkConfig.BEHAVIOR_ON_ERROR, "report",
                HttpSinkConfig.HTTP_RETRY_MAX, "0");

        task.put(List.of(record(1, "{\"a\":1}"), record(2, "{\"a\":2}")));

        assertEquals(3, server.count(), "1 次批量 + 2 次拆单");
        assertEquals("[{\"a\":1},{\"a\":2}]", server.exchanges().get(0).body());
        assertEquals("{\"a\":1}", server.exchanges().get(1).body());
    }

    /* ------------------------------------------------------------------ 路由 */

    @Test
    void routesEachTopicToItsOwnPath() {
        start(new FakeContext(), HttpSinkConfig.HTTP_URL, server.url("/sync/${topic}"));
        task.put(List.of(
                new SinkRecord("orders", 0, null, null, Schema.STRING_SCHEMA, "{}", 1L),
                new SinkRecord("users", 0, null, null, Schema.STRING_SCHEMA, "{}", 2L)));

        assertEquals(2, server.count());
        assertEquals("/sync/orders", server.exchanges().get(0).path());
        assertEquals("/sync/users", server.exchanges().get(1).path());
    }

    /* ------------------------------------------------------------------ 压缩 */

    @Test
    void gzipIsAppliedAndDecodable() throws IOException {
        start(new FakeContext(), HttpSinkConfig.HTTP_COMPRESSION, "gzip");
        task.put(List.of(record(1, "{\"a\":1}")));

        assertEquals("gzip", server.only().header("content-encoding"));

        // 直接验证编码结果，避免依赖测试端点的字符串解码
        final HttpTransport.Payload payload = HttpTransport.encode("{\"a\":1}", true);
        assertTrue(payload.gzipped());
        try (GZIPInputStream input = new GZIPInputStream(
                new java.io.ByteArrayInputStream(payload.bytes()))) {
            assertEquals("{\"a\":1}", new String(input.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    /* ------------------------------------------------------------------ 其它 */

    @Test
    void emptyBatchIsNoop() {
        start(new FakeContext());
        task.put(List.of());
        task.put(null);
        assertEquals(0, server.count());
    }

    @Test
    void stopIsIdempotent() {
        start(new FakeContext());
        task.stop();
        task.stop();
        assertEquals(0, server.count());
    }

    /** 配置错误必须在 start() 就炸，而不是等到第一条记录 */
    @Test
    void invalidConfigFailsAtStart() {
        task = new HttpSinkTask();
        task.initialize(new FakeContext());
        assertThrows(RuntimeException.class, () -> task.start(Map.of("http.url", "ftp://x/y")));
    }
}
