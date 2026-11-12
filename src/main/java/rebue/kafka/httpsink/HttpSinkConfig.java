package rebue.kafka.httpsink;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP Sink 的全部配置。
 *
 * <p>命名有两条约束：
 * <ul>
 *   <li>与 Kafka Connect / Confluent HTTP Sink 同名的键（{@code max.retries}、{@code retry.backoff.ms}、
 *       {@code topics}、{@code tasks.max}）保持一致，dbz 这类管控面注入的配置不用改就能生效；</li>
 *   <li>本插件特有的键统一挂 {@code http.} 前缀，避免和框架键撞名。</li>
 * </ul>
 *
 * <p>老版本的 {@code http.api.url} 仍被接受（值为空时回落到它），{@code http.header.content-type}
 * 会被提升为 Content-Type：老配置只改 {@code connector.class} 即可迁移。
 */
public final class HttpSinkConfig extends AbstractConfig {
    private static final Logger log = LoggerFactory.getLogger(HttpSinkConfig.class);

    /* ------------------------------------------------------------ 目标地址 */

    /** 目标 URL，支持 {@code ${topic}} 占位符 */
    public static final String HTTP_URL = "http.url";
    /** 老版本的目标 URL，已废弃，仅在 {@code http.url} 为空时使用 */
    public static final String LEGACY_HTTP_API_URL = "http.api.url";
    public static final String HTTP_METHOD = "http.method";
    public static final String HTTP_VERSION = "http.version";
    public static final String HTTP_CONTENT_TYPE = "http.content.type";
    /** 自定义请求头的前缀：{@code http.header.X-Tenant=...} */
    public static final String HTTP_HEADER_PREFIX = "http.header.";
    public static final String HTTP_USER_AGENT = "http.user.agent";
    public static final String HTTP_COMPRESSION = "http.compression";

    /* ------------------------------------------------------------ 超时与重试 */

    public static final String HTTP_CONNECT_TIMEOUT_MS = "http.connect.timeout.ms";
    public static final String HTTP_REQUEST_TIMEOUT_MS = "http.request.timeout.ms";
    /** 单条（或单批）记录的重试总预算，到点就放弃，避免拖住 poll 触发 rebalance */
    public static final String HTTP_RETRY_TOTAL_TIMEOUT_MS = "http.retry.total.timeout.ms";
    /** 与 Confluent HTTP Sink 同名：单次请求的重试次数（不含首次） */
    public static final String MAX_RETRIES = "max.retries";
    /** 与 Confluent HTTP Sink 同名：首次退避 */
    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    public static final String RETRY_BACKOFF_MAX_MS = "retry.backoff.max.ms";
    public static final String RETRY_JITTER_MS = "retry.jitter.ms";

    /* ------------------------------------------------------------ 行为 */

    /** 失败后的行为：fail（抛给框架，进 DLQ）/ log / ignore */
    public static final String BEHAVIOR_ON_ERROR = "behavior.on.error";
    public static final String BATCH_ENABLED = "http.batch.enabled";
    public static final String BATCH_MAX_SIZE = "http.batch.max.size";
    /** 批量模式下是否把每条记录包成 {"metadata":{...},"value":...}，以便下游做幂等 */
    public static final String BATCH_ENVELOPE = "http.batch.envelope";
    /** auto / string / json / bytes */
    public static final String VALUE_FORMAT = "value.format";
    public static final String VALUE_BYTES_ENCODING = "value.bytes.encoding";
    /** 把源 topic/partition/offset/timestamp/key 写进请求头，供下游幂等去重 */
    public static final String METADATA_HEADERS_ENABLE = "http.metadata.headers.enable";
    /** 把 Kafka 记录自带的 headers 透传成请求头（前缀 x-kafka-header-） */
    public static final String HEADER_FORWARD = "http.header.forward";

    /* ------------------------------------------------------------ 认证 / TLS */

    public static final String AUTH_TYPE = "http.auth.type";
    public static final String AUTH_USERNAME = "http.auth.username";
    public static final String AUTH_PASSWORD = "http.auth.password";
    public static final String AUTH_TOKEN = "http.auth.token";
    public static final String TLS_TRUSTSTORE_PATH = "http.tls.truststore.path";
    public static final String TLS_TRUSTSTORE_PASSWORD = "http.tls.truststore.password";
    public static final String TLS_TRUSTSTORE_TYPE = "http.tls.truststore.type";
    public static final String TLS_INSECURE_SKIP_VERIFY = "http.tls.insecure.skip.verify";

    /** 允许出现在 URL 里的占位符，其余一律在启动时报错 */
    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^}]*)}");

    private static final ConfigDef DEFINITION = buildDefinition();

    public HttpSinkConfig(final Map<String, ?> props) {
        super(DEFINITION, props);
        validateCrossField();
    }

    public static ConfigDef configDef() {
        return DEFINITION;
    }

    private static ConfigDef buildDefinition() {
        final ConfigDef def = new ConfigDef();

        def.define(HTTP_URL, Type.STRING, null, new UrlValidator(), Importance.HIGH,
                "目标 URL，必须 http/https。支持 ${topic} 占位符，按记录所属 topic 路由。");
        def.define(LEGACY_HTTP_API_URL, Type.STRING, null, new UrlValidator(), Importance.LOW,
                "已废弃，等价于 http.url；仅在 http.url 为空时生效。");
        def.define(HTTP_METHOD, Type.STRING, "POST", ValidString.in("POST", "PUT", "PATCH"), Importance.MEDIUM,
                "HTTP 方法，默认 POST。");
        def.define(HTTP_VERSION, Type.STRING, "HTTP_1_1", ValidString.in("HTTP_1_1", "HTTP_2"), Importance.LOW,
                "HTTP 协议版本。默认 HTTP_1_1：HTTP_2 对明文（h2c）依赖服务端支持，升级失败会直接连不上。");
        def.define(HTTP_CONTENT_TYPE, Type.STRING, "application/json", null, Importance.MEDIUM,
                "Content-Type 请求头，默认 application/json。");
        def.define(HTTP_USER_AGENT, Type.STRING, "kafka-connect-http-sink/" + Version.VERSION, null, Importance.LOW,
                "User-Agent 请求头。");
        def.define(HTTP_COMPRESSION, Type.STRING, "none", ValidString.in("none", "gzip"), Importance.LOW,
                "请求体压缩方式。gzip 需要下游支持 Content-Encoding: gzip。");

        def.define(HTTP_CONNECT_TIMEOUT_MS, Type.INT, 5_000, Range.atLeast(1), Importance.LOW,
                "建立连接的超时（毫秒）。");
        def.define(HTTP_REQUEST_TIMEOUT_MS, Type.INT, 30_000, Range.atLeast(1), Importance.MEDIUM,
                "单次请求的读超时（毫秒）。");
        def.define(HTTP_RETRY_TOTAL_TIMEOUT_MS, Type.INT, 120_000, Range.atLeast(0), Importance.MEDIUM,
                "单条记录的重试总预算（毫秒），到点即放弃。必须小于 consumer 的 max.poll.interval.ms，"
                        + "否则重试期间会被踢出消费组、触发 rebalance。0 表示不限制（不推荐）。");
        def.define(MAX_RETRIES, Type.INT, 10, Range.atLeast(0), Importance.MEDIUM,
                "单次投递的重试次数（不含首次）。与 Confluent HTTP Sink 同名，管控面注入即可生效。");
        def.define(RETRY_BACKOFF_MS, Type.INT, 3_000, Range.atLeast(0), Importance.MEDIUM,
                "首次退避（毫秒），之后按 2 的幂增长。");
        def.define(RETRY_BACKOFF_MAX_MS, Type.INT, 30_000, Range.atLeast(0), Importance.MEDIUM,
                "退避上限（毫秒）。");
        def.define(RETRY_JITTER_MS, Type.INT, 1_000, Range.atLeast(0), Importance.LOW,
                "退避抖动上限（毫秒），避免多个 task 同时重试打崩下游。");

        def.define(BEHAVIOR_ON_ERROR, Type.STRING, "fail", ValidString.in("fail", "log", "ignore"), Importance.HIGH,
                "失败后的行为。fail：抛给框架（配 errors.tolerance=all + DLQ 即进死信），默认；"
                        + "log：记日志后继续（会丢数据，不建议）；ignore：静默跳过（会丢数据，不建议）。");
        def.define(BATCH_ENABLED, Type.BOOLEAN, false, null, Importance.LOW,
                "是否把一批记录合成一个 JSON 数组请求。下游必须支持数组，且整批要么全成功要么全失败。");
        def.define(BATCH_MAX_SIZE, Type.INT, 100, Range.atLeast(1), Importance.LOW,
                "批量模式下单个请求最多包含的记录数。");
        def.define(BATCH_ENVELOPE, Type.BOOLEAN, false, null, Importance.LOW,
                "批量模式下是否把每条记录包成 {\"metadata\":{topic,partition,offset,timestamp},\"value\":...}；"
                        + "开启后下游才能按坐标去重，关闭则数组元素就是原始 value。");
        def.define(VALUE_FORMAT, Type.STRING, "auto", ValidString.in("auto", "string", "json", "bytes"),
                Importance.MEDIUM,
                "请求体的取值方式。auto：字符串原样输出、Struct/Map 转 JSON、byte[] 按 value.bytes.encoding；"
                        + "string：强制 toString；json：强制按 JSON 序列化；bytes：byte[] 直传 BASE64。");
        def.define(VALUE_BYTES_ENCODING, Type.STRING, "base64", ValidString.in("base64", "utf8"), Importance.LOW,
                "byte[] 型 value 的编码方式。");
        def.define(METADATA_HEADERS_ENABLE, Type.BOOLEAN, true, null, Importance.LOW,
                "是否带上 X-Kafka-Topic / X-Kafka-Partition / X-Kafka-Offset / X-Kafka-Timestamp / X-Kafka-Key，"
                        + "下游可据此做幂等去重。");
        def.define(HEADER_FORWARD, Type.BOOLEAN, false, null, Importance.LOW,
                "是否把记录自带的 headers 透传为 x-kafka-header-* 请求头（__connect.errors.* 会跳过）。");

        def.define(AUTH_TYPE, Type.STRING, "none", ValidString.in("none", "basic", "bearer"), Importance.MEDIUM,
                "认证方式：none / basic / bearer。");
        def.define(AUTH_USERNAME, Type.STRING, null, null, Importance.MEDIUM, "basic 认证的用户名。");
        def.define(AUTH_PASSWORD, Type.PASSWORD, null, null, Importance.MEDIUM, "basic 认证的密码。");
        def.define(AUTH_TOKEN, Type.PASSWORD, null, null, Importance.MEDIUM, "bearer 认证的令牌。");

        def.define(TLS_TRUSTSTORE_PATH, Type.STRING, null, null, Importance.LOW,
                "自定义信任库路径（JKS/PKCS12），用于自签证书。");
        def.define(TLS_TRUSTSTORE_PASSWORD, Type.PASSWORD, null, null, Importance.LOW, "信任库密码。");
        def.define(TLS_TRUSTSTORE_TYPE, Type.STRING, "JKS", ValidString.in("JKS", "PKCS12"), Importance.LOW,
                "信任库类型。");
        def.define(TLS_INSECURE_SKIP_VERIFY, Type.BOOLEAN, false, null, Importance.LOW,
                "跳过 TLS 证书校验。仅用于联调，生产环境开启等于放弃传输安全。");

        return def;
    }

    /**
     * 跨字段校验：ConfigDef 只能校验单键，这里补上与其它键相关的约束。
     *
     * <p>失败即 {@link ConfigException}，连接器会在 start 阶段直接 FAILED —— 比跑起来之后才在
     * 第一条记录上炸更容易定位。
     */
    private void validateCrossField() {
        final String url = urlTemplate();
        if (url == null || url.isEmpty()) {
            throw new ConfigException(HTTP_URL, null,
                    "必须配置 http.url（老配置可用已废弃的 http.api.url）");
        }
        final Matcher matcher = PLACEHOLDER.matcher(url);
        while (matcher.find()) {
            if (!"topic".equals(matcher.group(1))) {
                throw new ConfigException(HTTP_URL, url,
                        "不支持的占位符 ${" + matcher.group(1) + "}，目前只支持 ${topic}");
            }
        }

        final String authType = getString(AUTH_TYPE);
        if ("basic".equals(authType) && isBlank(getString(AUTH_USERNAME))) {
            throw new ConfigException(AUTH_USERNAME, null, "http.auth.type=basic 时必须配置用户名");
        }
        if ("basic".equals(authType) && getPassword(AUTH_PASSWORD) == null) {
            throw new ConfigException(AUTH_PASSWORD, null, "http.auth.type=basic 时必须配置密码");
        }
        if ("bearer".equals(authType) && getPassword(AUTH_TOKEN) == null) {
            throw new ConfigException(AUTH_TOKEN, null, "http.auth.type=bearer 时必须配置令牌");
        }
        if (getInt(RETRY_BACKOFF_MAX_MS) < getInt(RETRY_BACKOFF_MS)) {
            throw new ConfigException(RETRY_BACKOFF_MAX_MS, getInt(RETRY_BACKOFF_MAX_MS),
                    "退避上限不能小于首次退避（retry.backoff.ms=" + getInt(RETRY_BACKOFF_MS) + "）");
        }
        if (getBoolean(TLS_INSECURE_SKIP_VERIFY)) {
            log.warn("http.tls.insecure.skip.verify=true：已关闭 TLS 证书校验，仅可用于联调");
        }
        if (getBoolean(BATCH_ENABLED)) {
            log.info("批量投递已开启：单请求最多 {} 条，下游必须把整批当作一个原子单元", getInt(BATCH_MAX_SIZE));
        }
    }

    /* ------------------------------------------------------------ 派生值 */

    /** 目标 URL（优先 {@code http.url}，回落到已废弃的 {@code http.api.url}） */
    public String urlTemplate() {
        final String url = trimToNull(originals().get(HTTP_URL) == null ? null : getString(HTTP_URL));
        return url != null ? url : trimToNull(getString(LEGACY_HTTP_API_URL));
    }

    /** 把 {@code ${topic}} 替换成实际 topic；没有占位符时原样返回 */
    public String urlFor(final String topic) {
        return urlTemplate().replace("${topic}", topic == null ? "" : topic);
    }

    /**
     * {@code http.header.*} 收集到的自定义请求头。
     *
     * <p>{@code content-type} 会被单独取出（老配置用它设置 Content-Type），不重复放进请求头。
     */
    public Map<String, String> customHeaders() {
        final Map<String, String> headers = new LinkedHashMap<>();
        for (final Map.Entry<String, Object> entry : originalsWithPrefix(HTTP_HEADER_PREFIX).entrySet()) {
            final String name = entry.getKey();
            if (name == null || name.isBlank()) {
                continue;
            }
            if ("content-type".equalsIgnoreCase(name)) {
                continue;
            }
            headers.put(name, String.valueOf(entry.getValue()));
        }
        return headers;
    }

    /** Content-Type：显式配置优先，其次老配置的 {@code http.header.content-type} */
    public String contentType() {
        final Object legacy = originalsWithPrefix(HTTP_HEADER_PREFIX).get("content-type");
        if (legacy != null && !String.valueOf(legacy).isBlank()) {
            return String.valueOf(legacy);
        }
        return getString(HTTP_CONTENT_TYPE);
    }

    public int maxRetries() {
        return getInt(MAX_RETRIES);
    }

    public int retryBackoffMs() {
        return getInt(RETRY_BACKOFF_MS);
    }

    public int retryBackoffMaxMs() {
        return getInt(RETRY_BACKOFF_MAX_MS);
    }

    public int retryJitterMs() {
        return getInt(RETRY_JITTER_MS);
    }

    public int retryTotalTimeoutMs() {
        return getInt(HTTP_RETRY_TOTAL_TIMEOUT_MS);
    }

    public String basicUser() {
        return getString(AUTH_USERNAME);
    }

    public char[] basicPassword() {
        final org.apache.kafka.common.config.types.Password password = getPassword(AUTH_PASSWORD);
        return password == null ? null : password.value().toCharArray();
    }

    public char[] bearerToken() {
        final org.apache.kafka.common.config.types.Password token = getPassword(AUTH_TOKEN);
        return token == null ? null : token.value().toCharArray();
    }

    static boolean isBlank(final String text) {
        return text == null || text.isBlank();
    }

    private static String trimToNull(final String text) {
        if (text == null) {
            return null;
        }
        final String trimmed = text.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    /** URL 校验：允许占位符，但替换后必须能解析成 http/https 地址 */
    static final class UrlValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            if (value == null) {
                return;
            }
            final String text = String.valueOf(value).trim();
            if (text.isEmpty()) {
                return;
            }
            try {
                final URI uri = new URI(text.replace("${topic}", "probe"));
                final String scheme = uri.getScheme();
                if (scheme == null
                        || (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme))) {
                    throw new ConfigException(name, value, "只支持 http / https 协议");
                }
                if (uri.getHost() == null || uri.getHost().isBlank()) {
                    throw new ConfigException(name, value, "缺少主机名");
                }
            } catch (final URISyntaxException e) {
                throw new ConfigException(name, value, "不是合法的 URL: " + e.getMessage());
            }
        }

        @Override
        public String toString() {
            return "http/https URL";
        }
    }
}
