package rebue.kafka.httpsink;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.sun.net.httpserver.HttpServer;

/**
 * 测试用的 HTTP 端点。
 *
 * <p>用 JDK 自带的 {@link HttpServer} 而不是 mock：失败语义的全部价值都在"真实的连接、真实的
 * 状态码、真实的超时"上，mock 掉这些等于把要验证的东西一起 mock 掉了。
 */
final class TestServer implements AutoCloseable {

    /** 一次收到的请求 */
    record Exchange(String method, String path, Map<String, String> headers, String body) {
        String header(final String name) {
            return headers.get(name.toLowerCase(Locale.ROOT));
        }
    }

    private final HttpServer server;
    private final List<Exchange> exchanges = Collections.synchronizedList(new ArrayList<>());
    private final Queue<Integer> scripted = new ConcurrentLinkedQueue<>();

    private volatile int defaultStatus = 200;
    private volatile String responseBody = "{\"ok\":true}";
    private volatile long delayMs;
    private volatile String redirectTo;

    private TestServer(final HttpServer server) {
        this.server = server;
    }

    static TestServer start() throws IOException {
        final HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        final TestServer test = new TestServer(server);
        server.createContext("/", test::handle);
        server.start();
        return test;
    }

    /** 按顺序返回这些状态码，用完之后回落到 {@link #alwaysRespond} */
    TestServer scriptResponses(final int... statuses) {
        for (final int status : statuses) {
            scripted.add(status);
        }
        return this;
    }

    TestServer alwaysRespond(final int status) {
        this.defaultStatus = status;
        return this;
    }

    TestServer respondBody(final String body) {
        this.responseBody = body;
        return this;
    }

    TestServer redirectTo(final String location) {
        this.redirectTo = location;
        this.defaultStatus = 302;
        return this;
    }

    /** 每次请求前先睡一会儿，用于验证时间预算 */
    TestServer delay(final long millis) {
        this.delayMs = millis;
        return this;
    }

    String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    String url(final String path) {
        return url() + path;
    }

    List<Exchange> exchanges() {
        return List.copyOf(exchanges);
    }

    Exchange only() {
        if (exchanges.size() != 1) {
            throw new AssertionError("期望恰好 1 个请求，实际 " + exchanges.size() + " 个");
        }
        return exchanges.get(0);
    }

    int count() {
        return exchanges.size();
    }

    void reset() {
        exchanges.clear();
        scripted.clear();
    }

    private void handle(final com.sun.net.httpserver.HttpExchange exchange) throws IOException {
        try {
            if (delayMs > 0) {
                Thread.sleep(delayMs);
            }
            final String body;
            try (InputStream input = exchange.getRequestBody()) {
                body = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            }
            final Map<String, String> headers = new LinkedHashMap<>();
            exchange.getRequestHeaders().forEach((name, values) ->
                    headers.put(name.toLowerCase(Locale.ROOT), String.join(",", values)));
            exchanges.add(new Exchange(exchange.getRequestMethod(),
                    exchange.getRequestURI().getPath(), headers, body));

            final Integer next = scripted.poll();
            final int status = next == null ? defaultStatus : next;
            if (status >= 300 && status < 400 && redirectTo != null) {
                exchange.getResponseHeaders().add("Location", redirectTo);
            }
            final byte[] payload = responseBody.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, payload.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(payload);
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            exchange.sendResponseHeaders(500, -1);
        } finally {
            exchange.close();
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
