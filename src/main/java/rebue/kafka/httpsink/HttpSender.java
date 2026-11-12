package rebue.kafka.httpsink;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 把一条记录（或一批）投递到 HTTP 端点，并按响应语义决定「成功 / 可重试 / 不可重试」。
 *
 * <p>整条死信链路能否成立，全看这里的分类：
 * <ul>
 *   <li>2xx —— 成功，框架提交 offset；</li>
 *   <li>408 / 429 / 5xx / 网络异常 —— {@link RetriableException}，框架按
 *       {@code errors.retry.*} 重试，超时后写 DLQ；</li>
 *   <li>其它 4xx / 3xx —— {@link DataException}，一次就失败并写 DLQ（重试没有意义）。</li>
 * </ul>
 *
 * <p>插件自己也做一层有界重试（{@code max.retries} / {@code retry.backoff.ms}），两次重试
 * 之间是「业务请求级」的退避，比框架的整批重试粒度细；框架那层仍作为兜底。
 */
final class HttpSender {
    private static final Logger log = LoggerFactory.getLogger(HttpSender.class);

    /** 响应体最多带多少字符进异常与日志：够看清下游报什么错，又不会把日志打爆 */
    private static final int BODY_PREVIEW_LIMIT = 512;

    /**
     * HttpClient 的工作线程。
     *
     * <p>共享一个 daemon 池：每个 task 建一个池在任务频繁重建（配置变更会重建 task）时会
     * 泄漏线程。daemon 保证 JVM 退出不被阻塞。
     */
    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool(new java.util.concurrent.ThreadFactory() {
        private final AtomicInteger seq = new AtomicInteger();

        @Override
        public Thread newThread(final Runnable runnable) {
            final Thread thread = new Thread(runnable, "http-sink-" + seq.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    });

    private final HttpClient client;
    private final String method;
    private final String contentType;
    private final String userAgent;
    private final boolean gzip;
    private final Map<String, String> staticHeaders;
    private final int requestTimeoutMs;
    private final int maxRetries;
    private final int backoffMs;
    private final int backoffMaxMs;
    private final int jitterMs;
    private final int retryTotalTimeoutMs;
    private final boolean metadataHeadersEnabled;
    private final boolean headerForward;

    /** 这些头由 HttpClient 或安全策略掌控，不允许被记录头/配置覆盖 */
    private static final java.util.Set<String> RESERVED_HEADERS = java.util.Set.of(
            "host", "content-length", "connection", "transfer-encoding",
            "authorization", "expect", "upgrade", "cookie", "content-type");

    private HttpSender(final HttpSinkConfig config) {
        this.method = config.getString(HttpSinkConfig.HTTP_METHOD);
        this.contentType = config.contentType();
        this.userAgent = config.getString(HttpSinkConfig.HTTP_USER_AGENT);
        this.gzip = "gzip".equalsIgnoreCase(config.getString(HttpSinkConfig.HTTP_COMPRESSION));
        this.staticHeaders = buildStaticHeaders(config);
        this.requestTimeoutMs = config.getInt(HttpSinkConfig.HTTP_REQUEST_TIMEOUT_MS);
        this.maxRetries = config.maxRetries();
        this.backoffMs = config.retryBackoffMs();
        this.backoffMaxMs = config.retryBackoffMaxMs();
        this.jitterMs = config.retryJitterMs();
        this.retryTotalTimeoutMs = config.retryTotalTimeoutMs();
        this.metadataHeadersEnabled = config.getBoolean(HttpSinkConfig.METADATA_HEADERS_ENABLE);
        this.headerForward = config.getBoolean(HttpSinkConfig.HEADER_FORWARD);
        final HttpClient.Builder builder = HttpClient.newBuilder()
                .version("HTTP_2".equalsIgnoreCase(config.getString(HttpSinkConfig.HTTP_VERSION))
                        ? HttpClient.Version.HTTP_2 : HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofMillis(config.getInt(HttpSinkConfig.HTTP_CONNECT_TIMEOUT_MS)))
                // 不跟随重定向：POST 被 301/302 改写为 GET 是标准行为，会把整个请求语义丢掉；
                // 而且跟随重定向等于让下游决定我们连哪里（SSRF）
                .followRedirects(HttpClient.Redirect.NEVER)
                .executor(EXECUTOR);
        // sslContext(null) 会直接 NPE：没有自定义信任库时不要设，用 JVM 默认
        final SSLContext ssl = sslContext(config);
        if (ssl != null) {
            builder.sslContext(ssl);
        }
        this.client = builder.build();
    }

    static HttpSender create(final HttpSinkConfig config) {
        return new HttpSender(config);
    }

    /**
     * 投递一条记录。
     *
     * @return 最后一次尝试的结果；调用方按 {@link Result#success()} 决定是否抛异常
     */
    Result send(final String url, final SinkRecord record, final String body) {
        final Map<String, String> headers = new LinkedHashMap<>(staticHeaders);
        headers.put("Content-Type", contentType);
        headers.put("User-Agent", userAgent);
        if (record != null) {
            headers.putAll(recordHeaders(record));
        }

        final byte[] payload;
        try {
            payload = gzip ? gzip(body) : body.getBytes(StandardCharsets.UTF_8);
        } catch (final IOException e) {
            // 压缩失败是我们自己的问题，重试也没用
            return Result.failure(0, "请求体压缩失败: " + e.getMessage(), false, 0);
        }
        if (gzip) {
            headers.put("Content-Encoding", "gzip");
        }

        final long deadline = retryTotalTimeoutMs <= 0
                ? Long.MAX_VALUE
                : System.nanoTime() + retryTotalTimeoutMs * 1_000_000L;

        Result last = null;
        for (int attempt = 0; ; attempt++) {
            last = attemptOnce(url, headers, payload);
            if (last.success() || !last.retriable) {
                return last;
            }
            if (attempt >= maxRetries) {
                log.warn("投递 {} 重试已用尽({} 次)，放弃: {}", url, maxRetries, last.describe());
                return last;
            }
            final long sleepMs = nextBackoffMs(attempt, last);
            if (System.nanoTime() + sleepMs * 1_000_000L > deadline) {
                log.warn("投递 {} 的重试预算({}ms)已耗尽，放弃: {}",
                        url, retryTotalTimeoutMs, last.describe());
                return last;
            }
            try {
                Thread.sleep(sleepMs);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RetriableException("投递 " + url + " 等待重试时被中断", e);
            }
        }
    }

    private Result attemptOnce(final String url, final Map<String, String> headers, final byte[] payload) {
        final HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofMillis(requestTimeoutMs))
                .method(method, HttpRequest.BodyPublishers.ofByteArray(payload));
        try {
            headers.forEach(builder::header);
        } catch (final IllegalArgumentException e) {
            // header 名/值非法属于配置错误，重试无意义
            return Result.failure(0, "请求头非法: " + e.getMessage(), false, 0);
        }

        try {
            final HttpResponse<String> response = client.send(builder.build(),
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            final int status = response.statusCode();
            return new Result(status, preview(response.body()), null, isRetriableStatus(status),
                    retryAfterMs(response));
        } catch (final IOException e) {
            return Result.failure(0, e.getClass().getSimpleName() + ": " + e.getMessage(), true, 0);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            // 停机中：立刻结束，别让 task 卡在重试里
            throw new RetriableException("投递 " + url + " 被中断", e);
        }
    }

    /** 2xx 成功；408/429/5xx 值得重试；其余（含 3xx）都不该重试 */
    private static boolean isRetriableStatus(final int status) {
        return status == 408 || status == 429 || (status >= 500 && status <= 599);
    }

    /**
     * 下一次退避：指数增长 + 抖动，并尊重下游的 {@code Retry-After}。
     *
     * <p>抖动是必须的：多个 task 同时被同一个下游拒绝时，不加抖动会一起醒来一起打过去。
     */
    private long nextBackoffMs(final int attempt, final Result last) {
        if (last.retryAfterMs > 0) {
            return Math.min(last.retryAfterMs, backoffMaxMs);
        }
        final long exponential = (long) backoffMs << Math.min(attempt, 10);
        final long capped = Math.min(exponential, backoffMaxMs);
        if (jitterMs <= 0) {
            return capped;
        }
        return capped + ThreadLocalRandom.current().nextLong(jitterMs + 1L);
    }

    private static long retryAfterMs(final HttpResponse<?> response) {
        final String header = response.headers().firstValue("Retry-After").orElse(null);
        if (header == null || header.isBlank()) {
            return 0;
        }
        try {
            // Retry-After 只支持秒；HTTP-date 形式太罕见，忽略后按自己的退避走
            return Long.parseLong(header.trim()) * 1_000L;
        } catch (final NumberFormatException e) {
            return 0;
        }
    }

    /** 源坐标头：下游可据此做幂等去重（重试与重启都会重复投递同一条记录） */
    private Map<String, String> recordHeaders(final SinkRecord record) {
        final Map<String, String> headers = new LinkedHashMap<>();
        if (metadataHeadersEnabled) {
            put(headers, "X-Kafka-Topic", record.topic());
            put(headers, "X-Kafka-Partition",
                    record.kafkaPartition() == null ? "" : String.valueOf(record.kafkaPartition()));
            put(headers, "X-Kafka-Offset", String.valueOf(record.kafkaOffset()));
            put(headers, "X-Kafka-Timestamp",
                    record.timestamp() == null ? "" : String.valueOf(record.timestamp()));
            if (record.key() != null) {
                headers.put("X-Kafka-Key", String.valueOf(record.key()));
            }
        }
        if (headerForward && record.headers() != null) {
            for (final org.apache.kafka.connect.header.Header header : record.headers()) {
                final String name = header.key();
                if (name == null || name.isBlank()) {
                    continue;
                }
                // Connect 自己塞的错误上下文会让下游看到一堆内部信息
                if (name.startsWith("__connect.errors.") || name.startsWith("_connect.errors.")) {
                    continue;
                }
                if (RESERVED_HEADERS.contains(name.toLowerCase(java.util.Locale.ROOT))) {
                    continue;
                }
                headers.put("x-kafka-header-" + name,
                        header.value() == null ? "" : String.valueOf(header.value()));
            }
        }
        return headers;
    }

    private static void put(final Map<String, String> headers, final String name, final String value) {
        if (value != null && !value.isEmpty()) {
            headers.put(name, value);
        }
    }

    private static Map<String, String> buildStaticHeaders(final HttpSinkConfig config) {
        final Map<String, String> headers = new LinkedHashMap<>(config.customHeaders());
        if ("basic".equals(config.getString(HttpSinkConfig.AUTH_TYPE))) {
            final char[] password = config.basicPassword();
            final String pair = config.basicUser() + ":" + new String(password == null ? new char[0] : password);
            headers.put("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString(pair.getBytes(StandardCharsets.UTF_8)));
        } else if ("bearer".equals(config.getString(HttpSinkConfig.AUTH_TYPE))) {
            final char[] token = config.bearerToken();
            headers.put("Authorization", "Bearer " + (token == null ? "" : new String(token)));
        }
        return Map.copyOf(headers);
    }

    /**
     * TLS 上下文。
     *
     * <p>返回 null 表示用 JVM 默认（信任系统 CA）。自签证书配 truststore；
     * {@code insecure.skip.verify} 只给联调用，启动时已经 warn 过。
     */
    private static SSLContext sslContext(final HttpSinkConfig config) {
        final String truststorePath = config.getString(HttpSinkConfig.TLS_TRUSTSTORE_PATH);
        final boolean insecure = config.getBoolean(HttpSinkConfig.TLS_INSECURE_SKIP_VERIFY);
        if (!insecure && (truststorePath == null || truststorePath.isBlank())) {
            return null;
        }
        try {
            final SSLContext context = SSLContext.getInstance("TLS");
            if (insecure) {
                context.init(null, new TrustManager[]{new TrustAll()}, new SecureRandom());
                return context;
            }
            final String type = config.getString(HttpSinkConfig.TLS_TRUSTSTORE_TYPE);
            final KeyStore keyStore = KeyStore.getInstance(type);
            final org.apache.kafka.common.config.types.Password storePassword =
                    config.getPassword(HttpSinkConfig.TLS_TRUSTSTORE_PASSWORD);
            final char[] password = storePassword == null ? new char[0] : storePassword.value().toCharArray();
            try (InputStream input = Files.newInputStream(Path.of(truststorePath))) {
                keyStore.load(input, password);
            }
            final TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factory.init(keyStore);
            context.init(null, factory.getTrustManagers(), new SecureRandom());
            return context;
        } catch (final Exception e) {
            // 配置了信任库却加载失败，必须启动失败：否则会在第一条记录上以 SSL 异常出现，
            // 被误判成"下游不可用"而反复重试
            throw new ConnectException("加载信任库失败: " + truststorePath, e);
        }
    }

    private static byte[] gzip(final String body) throws IOException {
        final java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(body.getBytes(StandardCharsets.UTF_8));
        }
        return out.toByteArray();
    }

    private static String preview(final String body) {
        if (body == null) {
            return "";
        }
        final String oneLine = body.replace('\n', ' ').replace('\r', ' ');
        return oneLine.length() <= BODY_PREVIEW_LIMIT
                ? oneLine
                : oneLine.substring(0, BODY_PREVIEW_LIMIT) + "...";
    }

    /** 一条记录（或一批）投递的最终结果 */
    static final class Result {
        final int status;
        final String body;
        final String error;
        final boolean retriable;
        final long retryAfterMs;

        Result(final int status, final String body, final String error,
               final boolean retriable, final long retryAfterMs) {
            this.status = status;
            this.body = body == null ? "" : body;
            this.error = error;
            this.retriable = retriable;
            this.retryAfterMs = retryAfterMs;
        }

        static Result failure(final int status, final String error,
                              final boolean retriable, final long retryAfterMs) {
            return new Result(status, "", error, retriable, retryAfterMs);
        }

        boolean success() {
            return status >= 200 && status < 300;
        }

        String describe() {
            final StringBuilder sb = new StringBuilder();
            sb.append(status == 0 ? "无响应" : "HTTP " + status);
            if (error != null && !error.isEmpty()) {
                sb.append(" (").append(error).append(')');
            }
            if (!body.isEmpty()) {
                sb.append(" body=").append(body);
            }
            return sb.toString();
        }

        /**
         * 转成 Connect 认识的异常。
         *
         * <p>{@link RetriableException} 交给框架重试；{@link DataException} 是记录级问题，
         * 在 {@code errors.tolerance=all} 下会直接写 DLQ。
         */
        RuntimeException toException(final SinkRecord record) {
            final String where = record == null
                    ? ""
                    : " topic=" + record.topic() + " partition=" + record.kafkaPartition()
                    + " offset=" + record.kafkaOffset();
            final String message = "HTTP 投递失败" + where + ": " + describe();
            return retriable ? new RetriableException(message) : new DataException(message);
        }
    }

    /** 只用于联调的信任所有证书；生产开启等于放弃传输安全 */
    private static final class TrustAll extends X509ExtendedTrustManager {
        @Override
        public void checkClientTrusted(final X509Certificate[] chain, final String authType) {
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] chain, final String authType) {
        }

        @Override
        public void checkClientTrusted(final X509Certificate[] chain, final String authType,
                                       final java.net.Socket socket) {
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] chain, final String authType,
                                       final java.net.Socket socket) {
        }

        @Override
        public void checkClientTrusted(final X509Certificate[] chain, final String authType,
                                       final javax.net.ssl.SSLEngine engine) {
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] chain, final String authType,
                                       final javax.net.ssl.SSLEngine engine) {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
