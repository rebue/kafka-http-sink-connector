package rebue.kafka.httpsink.support;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

import rebue.kafka.httpsink.HttpSinkConfig;

/**
 * 请求头装配。
 *
 * <p>分三层，优先级从低到高：静态头（Content-Type / User-Agent / 认证 / {@code http.headers.*}）→
 * 源坐标头（供下游做幂等）→ 透传的 Kafka 记录头。
 *
 * <p>两条纪律：
 * <ul>
 *   <li>由客户端或安全策略掌控的头（Host、Content-Length、Authorization…）不允许被配置或记录头
 *       覆盖，否则一个上游记录就能改写我们发往下游的身份；</li>
 *   <li>记录头来源于上游任意字节，写进 HTTP 头之前必须去掉 CR/LF —— 头注入是真实存在的攻击面，
 *       而 JDK 的 HttpClient 会直接抛异常，把一次投递变成一条死信。</li>
 * </ul>
 */
public final class RequestHeaders {
    /** 幂等键：下游用它去重，比自己去拼 topic/partition/offset 三个头更省事 */
    public static final String IDEMPOTENCY_KEY = "X-Idempotency-Key";
    public static final String KAFKA_HEADER_PREFIX = "x-kafka-header-";
    public static final String FORWARD_ALL = "*";

    private static final int KEY_LIMIT = 512;

    /** 客户端负责或与安全相关的头，禁止被配置/记录头覆盖 */
    private static final Set<String> RESERVED = Set.of(
            "host", "content-length", "content-type", "connection", "transfer-encoding",
            "authorization", "proxy-authorization", "expect", "upgrade", "cookie",
            "te", "trailer", "keep-alive");

    private final Map<String, String> staticHeaders;
    private final boolean metadataEnabled;
    private final boolean forwardAll;
    private final Set<String> forwardedNames;

    private RequestHeaders(final Map<String, String> staticHeaders, final boolean metadataEnabled,
                           final boolean forwardAll, final Set<String> forwardedNames) {
        this.staticHeaders = staticHeaders;
        this.metadataEnabled = metadataEnabled;
        this.forwardAll = forwardAll;
        this.forwardedNames = forwardedNames;
    }

    public static RequestHeaders of(final HttpSinkConfig config) {
        final Map<String, String> headers = new LinkedHashMap<>(config.customHeaders());
        headers.put("Content-Type", config.getString(HttpSinkConfig.HTTP_CONTENT_TYPE));
        headers.put("User-Agent", config.getString(HttpSinkConfig.HTTP_USER_AGENT));
        applyAuth(config, headers);

        final var forward = config.getList(HttpSinkConfig.HTTP_HEADER_FORWARD);
        final boolean all = forward.contains(FORWARD_ALL);
        final Set<String> names = all ? Set.of() : Set.copyOf(forward);
        return new RequestHeaders(Map.copyOf(headers), config.getBoolean(HttpSinkConfig.HTTP_METADATA_HEADERS),
                all, names);
    }

    private static void applyAuth(final HttpSinkConfig config, final Map<String, String> headers) {
        switch (config.authType()) {
            case BASIC -> {
                final String password = secret(config.basicPassword());
                final String pair = config.basicUser() + ":" + password;
                headers.put("Authorization", "Basic "
                        + Base64.getEncoder().encodeToString(pair.getBytes(StandardCharsets.UTF_8)));
            }
            case BEARER -> headers.put("Authorization", "Bearer " + secret(config.bearerToken()));
            case NONE -> {
                // 不设 Authorization
            }
        }
    }

    private static String secret(final char[] value) {
        return value == null ? "" : new String(value);
    }

    /** 一条记录的完整请求头 */
    public Map<String, String> forRecord(final SinkRecord record) {
        final Map<String, String> headers = new LinkedHashMap<>(staticHeaders);
        if (record == null) {
            return headers;
        }
        if (metadataEnabled) {
            put(headers, "X-Kafka-Topic", record.topic());
            put(headers, "X-Kafka-Partition", asString(record.kafkaPartition()));
            put(headers, "X-Kafka-Offset", String.valueOf(record.kafkaOffset()));
            put(headers, "X-Kafka-Timestamp", record.timestamp() == null ? null : record.timestamp().toString());
            put(headers, "X-Kafka-Key", truncate(sanitize(asString(record.key()))));
            put(headers, IDEMPOTENCY_KEY, idempotencyKey(record));
        }
        forwardRecordHeaders(record, headers);
        return headers;
    }

    /**
     * 批量投递的请求头：一批跨多条记录，源坐标会失真，所以只带静态头。
     *
     * <p>需要幂等时打开 {@code http.batch.envelope=true}，把坐标放进报文里。
     */
    public Map<String, String> forBatch() {
        return new LinkedHashMap<>(staticHeaders);
    }

    public Map<String, String> staticHeaders() {
        return staticHeaders;
    }

    /** 幂等键的取值：Kafka 里一条记录的唯一坐标 */
    public static String idempotencyKey(final SinkRecord record) {
        return record.topic() + "-" + record.kafkaPartition() + "-" + record.kafkaOffset();
    }

    private void forwardRecordHeaders(final SinkRecord record, final Map<String, String> target) {
        if (!forwardAll && forwardedNames.isEmpty()) {
            return;
        }
        if (record.headers() == null) {
            return;
        }
        for (final Header header : record.headers()) {
            final String name = header.key();
            if (name == null || name.isBlank()) {
                continue;
            }
            // Connect 自己塞的错误上下文会把内部信息带给下游
            if (name.startsWith("__connect.errors.") || name.startsWith("_connect.errors.")) {
                continue;
            }
            if (!forwardAll && !forwardedNames.contains(name)) {
                continue;
            }
            final String targetName = KAFKA_HEADER_PREFIX + sanitizeName(name);
            put(target, targetName, header.value() == null ? "" : sanitize(asString(header.value())));
        }
    }

    private static void put(final Map<String, String> headers, final String name, final String value) {
        if (name != null && value != null && !value.isEmpty()) {
            headers.put(name, value);
        }
    }

    private static String asString(final Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[] bytes) {
            return Base64.getEncoder().encodeToString(bytes);
        }
        return String.valueOf(value);
    }

    /** 去掉 CR/LF 与其他控制字符：头注入防线，也避免 HttpClient 直接抛异常 */
    public static String sanitize(final String value) {
        if (value == null) {
            return null;
        }
        final StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            final char ch = value.charAt(i);
            sb.append(ch < 0x20 || ch == 0x7f ? ' ' : ch);
        }
        return sb.toString();
    }

    private static String sanitizeName(final String name) {
        final StringBuilder sb = new StringBuilder(name.length());
        for (int i = 0; i < name.length(); i++) {
            final char ch = name.charAt(i);
            sb.append(isTokenChar(ch) ? ch : '_');
        }
        return sb.toString();
    }

    private static String truncate(final String value) {
        if (value == null || value.length() <= KEY_LIMIT) {
            return value;
        }
        return value.substring(0, KEY_LIMIT) + "...";
    }

    /* ------------------------------------------------------------------ 供配置校验复用 */

    public static boolean isReserved(final String name) {
        return name != null && RESERVED.contains(name.toLowerCase(Locale.ROOT));
    }

    /** RFC 7230 token：合法头名只能由这些字符组成 */
    public static boolean isToken(final String name) {
        if (name == null || name.isEmpty()) {
            return false;
        }
        for (int i = 0; i < name.length(); i++) {
            if (!isTokenChar(name.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isTokenChar(final char ch) {
        if (ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9') {
            return true;
        }
        return "!#$%&'*+-.^_`|~".indexOf(ch) >= 0;
    }
}
