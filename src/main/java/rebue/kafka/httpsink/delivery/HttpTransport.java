package rebue.kafka.httpsink.delivery;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;

import rebue.kafka.httpsink.HttpSinkConfig;

/**
 * 一次 HTTP 尝试。
 *
 * <p>只负责"发出去、把响应翻译成 {@link DeliveryResult}"，不重试、不判断是否该重试 ——
 * 那是 {@link RetryPolicy} 的事。这样重试策略可以脱离网络被单测覆盖。
 *
 * <p>响应体按**上限读取**：下游一个 200 + 100MB 的响应不该把 sink 的堆打满，失败时也只需要
 * 前几百字节就能看清报什么错。
 */
public final class HttpTransport {
    private static final int BODY_PREVIEW_BYTES = 1024;

    /**
     * HttpClient 的工作线程。
     *
     * <p>全局共享一个 daemon 池：task 会随配置变更反复重建，每个 task 建一个池就是线程泄漏；
     * daemon 保证 JVM 退出时不被这些线程挡住。
     */
    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool(new ThreadFactory() {
        private final AtomicInteger sequence = new AtomicInteger();

        @Override
        public Thread newThread(final Runnable runnable) {
            final Thread thread = new Thread(runnable, "http-sink-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    });

    /**
     * 已编码好的请求体。
     *
     * <p>压缩放在重试循环之外：同一份 body 重试多次没必要每次重新压缩。
     */
    public record Payload(byte[] bytes, boolean gzipped) {
    }

    private final HttpClient client;
    private final String method;

    public HttpTransport(final HttpSinkConfig config) {
        this.method = config.getString(HttpSinkConfig.HTTP_METHOD);
        final HttpClient.Builder builder = HttpClient.newBuilder()
                .version(config.httpVersion())
                .connectTimeout(Duration.ofMillis(config.getInt(HttpSinkConfig.HTTP_CONNECT_TIMEOUT_MS)))
                // 不跟随重定向：POST 被 301/302 改写成 GET 是标准行为，会把请求语义整个丢掉；
                // 跟随重定向也等于让下游决定我们去连哪里
                .followRedirects(HttpClient.Redirect.NEVER)
                .executor(EXECUTOR);
        final javax.net.ssl.SSLContext ssl = TlsSupport.create(config);
        if (ssl != null) {
            builder.sslContext(ssl);
        }
        this.client = builder.build();
    }

    /** 按配置决定是否 gzip；放在重试循环外调用一次 */
    public static Payload encode(final String body, final boolean gzip) {
        final byte[] raw = body.getBytes(StandardCharsets.UTF_8);
        if (!gzip) {
            return new Payload(raw, false);
        }
        try {
            final java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream(raw.length / 2 + 64);
            try (GZIPOutputStream zipped = new GZIPOutputStream(out)) {
                zipped.write(raw);
            }
            return new Payload(out.toByteArray(), true);
        } catch (final IOException e) {
            // 内存流不会真的抛，但签名要求处理
            throw new ConnectException("请求体压缩失败", e);
        }
    }

    /**
     * 发一次请求。
     *
     * @param timeoutMs 本次尝试的超时：由调用方按剩余预算裁剪，避免一次请求吃穿整个预算
     * @throws RetriableException 线程被中断（停机中），调用方不应继续重试
     */
    public DeliveryResult send(final Payload payload, final URI uri, final Map<String, String> headers,
                               final int timeoutMs) {
        final long startedAt = System.nanoTime();
        final HttpRequest request;
        try {
            final HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                    .timeout(Duration.ofMillis(timeoutMs))
                    .method(method, HttpRequest.BodyPublishers.ofByteArray(payload.bytes()));
            headers.forEach(builder::header);
            if (payload.gzipped()) {
                builder.header("Content-Encoding", "gzip");
            }
            request = builder.build();
        } catch (final IllegalArgumentException e) {
            // 非法头名/头值：配置问题，重试无意义
            return DeliveryResult.poison("请求构造失败: " + e.getMessage(), 0, elapsedMs(startedAt));
        }

        try {
            final HttpResponse<InputStream> response =
                    client.send(request, HttpResponse.BodyHandlers.ofInputStream());
            final String preview = preview(response);
            return DeliveryResult.fromStatus(response.statusCode(), preview,
                    retryAfterMs(response), 1, elapsedMs(startedAt));
        } catch (final java.net.http.HttpTimeoutException e) {
            return DeliveryResult.transportFailure("请求超时(" + timeoutMs + "ms)", 1, elapsedMs(startedAt));
        } catch (final IOException e) {
            return DeliveryResult.transportFailure(e.getClass().getSimpleName() + ": " + e.getMessage(),
                    1, elapsedMs(startedAt));
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetriableException("投递 " + uri + " 时被中断", e);
        }
    }

    /**
     * 读响应体前若干字节。
     *
     * <p>成功响应不需要内容，读一点就关掉既让连接可复用，也不会因为下游返回大报文而打满堆。
     */
    private static String preview(final HttpResponse<InputStream> response) {
        try (InputStream body = response.body()) {
            final byte[] head = body.readNBytes(BODY_PREVIEW_BYTES);
            return new String(head, StandardCharsets.UTF_8);
        } catch (final IOException e) {
            return "";
        }
    }

    private static long retryAfterMs(final HttpResponse<?> response) {
        final String header = response.headers().firstValue("Retry-After").orElse(null);
        if (header == null || header.isBlank()) {
            return 0;
        }
        try {
            // 只支持秒数形式；HTTP-date 太罕见，忽略后按本地退避走
            return Math.max(0, Long.parseLong(header.trim())) * 1_000L;
        } catch (final NumberFormatException e) {
            return 0;
        }
    }

    private static long elapsedMs(final long startedAt) {
        return (System.nanoTime() - startedAt) / 1_000_000L;
    }
}
