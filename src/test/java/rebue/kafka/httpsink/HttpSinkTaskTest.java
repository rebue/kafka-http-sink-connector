package rebue.kafka.httpsink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * 用 JDK 自带的 HttpServer 做端到端验证：不引第三方 mock 框架，插件本身也保持零运行时依赖。
 *
 * <p>重点不是覆盖率，而是把「死信链路成立的前提」钉死：
 * 4xx 必须抛不可重试异常、5xx 必须重试后抛可重试异常、2xx 必须不抛。
 */
class HttpSinkTaskTest {

    private TestServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    /* ------------------------------------------------------------ 成功路径 */

    @Test
    void sendsJsonBodyWithMetadataHeaders() throws Exception {
        server = new TestServer();
        server.reply(200, "ok");
        final HttpSinkTask task = startTask(props(server.url()));

        task.put(List.of(record("dbz.orders", 3, 42L, "{\"id\":1001}")));

        final Exchange exchange = server.single();
        assertEquals("POST", exchange.method);
        assertEquals("/sink", exchange.path);
        assertEquals("{\"id\":1001}", exchange.body);
        assertEquals("application/json", exchange.header("Content-Type"));
        assertEquals("dbz.orders", exchange.header("X-Kafka-Topic"));
        assertEquals("3", exchange.header("X-Kafka-Partition"));
        assertEquals("42", exchange.header("X-Kafka-Offset"));
        assertTrue(exchange.header("User-Agent").startsWith("kafka-connect-http-sink/"));
    }

    @Test
    void structValueIsRenderedAsJsonWithLogicalTypes() throws Exception {
        server = new TestServer();
        server.reply(200, "ok");
        final HttpSinkTask task = startTask(props(server.url()));

        final Schema schema = SchemaBuilder.struct().name("order")
                .field("id", Schema.INT64_SCHEMA)
                .field("amount", Decimal.schema(2))
                .field("note", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        final Struct struct = new Struct(schema)
                .put("id", 1001L)
                .put("amount", new BigDecimal("12.34"));

        task.put(List.of(new SinkRecord("dbz.orders", 0, null, null, schema, struct, 7L)));

        // Struct.toString() 会是 Struct{id=1001, ...}；这里必须是合法 JSON，且 Decimal 不能被转成 BASE64
        assertEquals("{\"id\":1001,\"amount\":12.34,\"note\":null}", server.single().body);
    }

    @Test
    void tombstoneIsSkippedWithoutAnyRequest() throws Exception {
        server = new TestServer();
        final HttpSinkTask task = startTask(props(server.url()));

        task.put(List.of(new SinkRecord("dbz.orders", 0, null, null, null, null, 9L)));

        assertEquals(0, server.exchanges.size());
    }

    @Test
    void urlTemplateUsesTopic() throws Exception {
        server = new TestServer();
        server.reply(200, "ok");
        final Map<String, String> props = props(server.url() + "/topics/${topic}");
        final HttpSinkTask task = startTask(props);

        task.put(List.of(record("dbz.orders", 0, 1L, "{}")));

        assertEquals("/sink/topics/dbz.orders", server.single().path);
    }

    /* ------------------------------------------------------------ 失败路径 */

    @Test
    void clientErrorIsDataExceptionAndNotRetried() throws Exception {
        server = new TestServer();
        server.reply(400, "{\"result\":-2}");
        final HttpSinkTask task = startTask(props(server.url()));

        final DataException error = assertThrows(DataException.class,
                () -> task.put(List.of(record("dbz.orders", 0, 1L, "{}"))));

        assertEquals(1, server.exchanges.size(), "4xx 不该重试");
        assertTrue(error.getMessage().contains("HTTP 400"));
        assertTrue(error.getMessage().contains("\"result\":-2"), "响应体要带进异常，页面才看得到原因");
    }

    @Test
    void serverErrorIsRetriedThenSucceeds() throws Exception {
        server = new TestServer();
        server.reply(503, "busy");
        server.reply(503, "busy");
        server.reply(200, "ok");
        final Map<String, String> props = props(server.url());
        props.put("max.retries", "3");
        props.put("retry.backoff.ms", "10");
        final HttpSinkTask task = startTask(props);

        task.put(List.of(record("dbz.orders", 0, 1L, "{\"id\":1}")));

        assertEquals(3, server.exchanges.size());
    }

    @Test
    void serverErrorExhaustedIsRetriableException() throws Exception {
        server = new TestServer();
        for (int i = 0; i < 5; i++) {
            server.reply(500, "boom");
        }
        final Map<String, String> props = props(server.url());
        props.put("max.retries", "2");
        props.put("retry.backoff.ms", "5");
        final HttpSinkTask task = startTask(props);

        final RetriableException error = assertThrows(RetriableException.class,
                () -> task.put(List.of(record("dbz.orders", 0, 1L, "{}"))));

        assertEquals(3, server.exchanges.size(), "首次 + 2 次重试");
        assertTrue(error.getMessage().contains("HTTP 500"));
    }

    @Test
    void requestTimeoutIsRetriable() throws Exception {
        server = new TestServer();
        server.delayMs = 600;
        server.reply(200, "late");
        final Map<String, String> props = props(server.url());
        props.put("http.request.timeout.ms", "120");
        props.put("max.retries", "0");
        final HttpSinkTask task = startTask(props);

        assertThrows(RetriableException.class,
                () -> task.put(List.of(record("dbz.orders", 0, 1L, "{}"))));
    }

    /* ------------------------------------------------------------ 行为开关 */

    @Test
    void logBehaviorSwallowsFailure() throws Exception {
        server = new TestServer();
        server.reply(400, "bad");
        final Map<String, String> props = props(server.url());
        props.put("behavior.on.error", "log");
        final HttpSinkTask task = startTask(props);

        // 不抛异常 = offset 会被提交，这条数据就丢了。只有明确接受这一点才该用 log
        task.put(List.of(record("dbz.orders", 0, 1L, "{}")));
        assertEquals(1, server.exchanges.size());
    }

    @Test
    void basicAuthAndCustomHeadersAreSent() throws Exception {
        server = new TestServer();
        server.reply(200, "ok");
        final Map<String, String> props = props(server.url());
        props.put("http.auth.type", "basic");
        props.put("http.auth.username", "dbz");
        props.put("http.auth.password", "s3cret");
        props.put("http.header.X-Tenant", "acme");
        final HttpSinkTask task = startTask(props);

        task.put(List.of(record("dbz.orders", 0, 1L, "{}")));

        final Exchange exchange = server.single();
        assertEquals("acme", exchange.header("X-Tenant"));
        // dbz:s3cret
        assertEquals("Basic ZGJ6OnMzY3JldA==", exchange.header("Authorization"));
    }

    @Test
    void recordHeadersAreForwardedWhenEnabled() throws Exception {
        server = new TestServer();
        server.reply(200, "ok");
        final Map<String, String> props = props(server.url());
        props.put("http.header.forward", "true");
        final HttpSinkTask task = startTask(props);

        final org.apache.kafka.connect.header.Headers headers = new ConnectHeaders()
                .addString("tenant", "acme")
                .addString("__connect.errors.topic", "should-not-leak");
        final SinkRecord record = new SinkRecord("dbz.orders", 0, null, null,
                Schema.STRING_SCHEMA, "{}", 5L, 1700000000000L,
                org.apache.kafka.common.record.TimestampType.CREATE_TIME, headers);

        task.put(List.of(record));

        final Exchange exchange = server.single();
        assertEquals("acme", exchange.header("x-kafka-header-tenant"));
        assertNull(exchange.header("x-kafka-header-__connect.errors.topic"));
    }

    @Test
    void gzipCompressionIsApplied() throws Exception {
        server = new TestServer();
        server.reply(200, "ok");
        final Map<String, String> props = props(server.url());
        props.put("http.compression", "gzip");
        final HttpSinkTask task = startTask(props);

        task.put(List.of(record("dbz.orders", 0, 1L, "{\"id\":1}")));

        final Exchange exchange = server.single();
        assertEquals("gzip", exchange.header("Content-Encoding"));
        try (InputStream in = new GZIPInputStream(new java.io.ByteArrayInputStream(exchange.rawBody))) {
            assertEquals("{\"id\":1}", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    /* ------------------------------------------------------------ 批量 */

    @Test
    void batchSendsJsonArray() throws Exception {
        server = new TestServer();
        server.reply(200, "ok");
        final Map<String, String> props = props(server.url());
        props.put("http.batch.enabled", "true");
        final HttpSinkTask task = startTask(props);

        task.put(List.of(
                record("dbz.orders", 0, 1L, "{\"id\":1}"),
                record("dbz.orders", 0, 2L, "{\"id\":2}")));

        assertEquals("[{\"id\":1},{\"id\":2}]", server.single().body);
    }

    @Test
    void batchEnvelopeCarriesCoordinates() throws Exception {
        server = new TestServer();
        server.reply(200, "ok");
        final Map<String, String> props = props(server.url());
        props.put("http.batch.enabled", "true");
        props.put("http.batch.envelope", "true");
        final HttpSinkTask task = startTask(props);

        task.put(List.of(record("dbz.orders", 2, 11L, "{\"id\":1}")));

        final String body = server.single().body;
        assertTrue(body.contains("\"metadata\":{\"topic\":\"dbz.orders\",\"partition\":2,\"offset\":11"));
        assertTrue(body.contains("\"value\":{\"id\":1}"));
    }

    /* ------------------------------------------------------------ 配置校验 */

    @Test
    void rejectsNonHttpUrl() {
        final Map<String, String> props = props("ftp://example.com/x");
        assertThrows(ConfigException.class, () -> new HttpSinkConfig(props));
    }

    @Test
    void rejectsUnknownPlaceholder() {
        final Map<String, String> props = props("http://example.com/${key}");
        assertThrows(ConfigException.class, () -> new HttpSinkConfig(props));
    }

    @Test
    void basicAuthRequiresCredentials() {
        final Map<String, String> props = props("http://example.com/x");
        props.put("http.auth.type", "basic");
        assertThrows(ConfigException.class, () -> new HttpSinkConfig(props));
    }

    @Test
    void requiresSomeUrl() {
        final Map<String, String> props = new HashMap<>();
        assertThrows(ConfigException.class, () -> new HttpSinkConfig(props));
    }

    @Test
    void legacyApiUrlStillWorks() throws Exception {
        server = new TestServer();
        server.reply(200, "ok");
        final Map<String, String> props = new HashMap<>();
        props.put("http.api.url", server.url());
        props.put("max.retries", "0");
        props.put("retry.jitter.ms", "0");

        final HttpSinkTask task = startTask(props);
        task.put(List.of(record("dbz.orders", 0, 1L, "{}")));

        assertEquals(1, server.exchanges.size());
    }

    /* ------------------------------------------------------------ 辅助 */

    private static Map<String, String> props(final String url) {
        final Map<String, String> props = new HashMap<>();
        props.put("http.url", url);
        props.put("max.retries", "0");
        props.put("retry.backoff.ms", "10");
        props.put("retry.jitter.ms", "0");
        props.put("http.connect.timeout.ms", "1000");
        props.put("http.request.timeout.ms", "2000");
        return props;
    }

    private static HttpSinkTask startTask(final Map<String, String> props) {
        final HttpSinkTask task = new HttpSinkTask();
        task.start(props);
        return task;
    }

    private static SinkRecord record(final String topic, final int partition,
                                     final long offset, final String json) {
        return new SinkRecord(topic, partition, null, null, Schema.STRING_SCHEMA, json, offset);
    }

    /** 记录一次请求；响应按队列顺序返回，队列空时默认 200 */
    private static final class Exchange {
        final String method;
        final String path;
        final Map<String, String> headers;
        final String body;
        final byte[] rawBody;

        Exchange(final String method, final String path,
                 final Map<String, String> headers, final byte[] rawBody) {
            this.method = method;
            this.path = path;
            this.headers = headers;
            this.rawBody = rawBody;
            this.body = new String(rawBody, StandardCharsets.UTF_8);
        }

        String header(final String name) {
            return headers.get(name.toLowerCase(java.util.Locale.ROOT));
        }
    }

    private static final class TestServer implements AutoCloseable {
        private final HttpServer server;
        private final List<Exchange> exchanges = new CopyOnWriteArrayList<>();
        private final Queue<int[]> replies = new ConcurrentLinkedQueue<>();
        private final Queue<String> replyBodies = new ConcurrentLinkedQueue<>();
        private volatile long delayMs;

        TestServer() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", this::handle);
            server.setExecutor(Executors.newCachedThreadPool(runnable -> {
                final Thread thread = new Thread(runnable, "test-http");
                thread.setDaemon(true);
                return thread;
            }));
            server.start();
        }

        String url() {
            return "http://127.0.0.1:" + server.getAddress().getPort() + "/sink";
        }

        void reply(final int status, final String body) {
            replies.add(new int[]{status});
            replyBodies.add(body);
        }

        Exchange single() {
            assertEquals(1, exchanges.size(), "期望恰好 1 次请求，实际 " + exchanges.size());
            return exchanges.get(0);
        }

        private void handle(final HttpExchange exchange) throws IOException {
            final byte[] body;
            try (InputStream in = exchange.getRequestBody()) {
                body = in.readAllBytes();
            }
            final Map<String, String> headers = new HashMap<>();
            exchange.getRequestHeaders().forEach((name, values) -> {
                if (!values.isEmpty()) {
                    headers.put(name.toLowerCase(java.util.Locale.ROOT), values.get(0));
                }
            });
            exchanges.add(new Exchange(exchange.getRequestMethod(),
                    exchange.getRequestURI().getPath(), headers, body));

            if (delayMs > 0) {
                try {
                    Thread.sleep(delayMs);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            final int[] status = replies.poll();
            final String replyBody = replyBodies.poll();
            final byte[] responseBody = (replyBody == null ? "" : replyBody).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status == null ? 200 : status[0], responseBody.length);
            exchange.getResponseBody().write(responseBody);
            exchange.close();
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
