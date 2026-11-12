package rebue.kafka.httpsink;

import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.config.ConfigException;

import rebue.kafka.httpsink.json.PayloadFormat;
import rebue.kafka.httpsink.route.UrlTemplate;
import rebue.kafka.httpsink.support.RequestHeaders;

/**
 * 连接器配置。
 *
 * <p>三条设计原则，都是拿线上问题换来的：
 *
 * <ol>
 *   <li><b>键名只守一个命名空间</b>：{@code http.*} 全是插件自己的语义；Kafka Connect 的
 *       {@code errors.*}、{@code consumer.*} 归框架。两套混用会出现"页面上填了、实际没人读"
 *       的假配置。</li>
 *   <li><b>能在启动期报的错，绝不拖到第一条记录</b>。地址写错、认证缺密码、批量配了纯文本，
 *       这些都在 {@code PUT /connectors} 时返回 400，而不是让 task 反复 FAILED 重启。</li>
 *   <li><b>跨字段约束集中在 {@link #problems} 里</b>，连接器的 {@code validate()} 直接复用，
 *       保证"页面校验"和"真正启动"用的是同一套规则。</li>
 * </ol>
 */
public final class HttpSinkConfig extends AbstractConfig {

    /* ------------------------------------------------------------------ 目标地址 */

    public static final String HTTP_URL = "http.url";
    public static final String HTTP_METHOD = "http.method";
    public static final String HTTP_PROTOCOL = "http.protocol";
    public static final String HTTP_CONTENT_TYPE = "http.content.type";
    public static final String HTTP_USER_AGENT = "http.user.agent";
    public static final String HTTP_COMPRESSION = "http.compression";

    /* ------------------------------------------------------------------ 超时与重试 */

    public static final String HTTP_CONNECT_TIMEOUT_MS = "http.connect.timeout.ms";
    public static final String HTTP_REQUEST_TIMEOUT_MS = "http.request.timeout.ms";
    public static final String HTTP_DELIVERY_BUDGET_MS = "http.delivery.budget.ms";
    public static final String HTTP_RETRY_MAX = "http.retry.max";
    public static final String HTTP_RETRY_BACKOFF_MS = "http.retry.backoff.ms";
    public static final String HTTP_RETRY_BACKOFF_MAX_MS = "http.retry.backoff.max.ms";
    public static final String HTTP_RETRY_JITTER_MS = "http.retry.jitter.ms";

    /* ------------------------------------------------------------------ 请求头 */

    /** 动态头前缀：{@code http.headers.X-Tenant=acme} */
    public static final String HTTP_HEADER_PREFIX = "http.headers.";
    public static final String HTTP_METADATA_HEADERS = "http.metadata.headers.enabled";
    public static final String HTTP_HEADER_FORWARD = "http.header.forward";

    /* ------------------------------------------------------------------ 报文 */

    public static final String HTTP_PAYLOAD_FORMAT = "http.payload.format";
    public static final String HTTP_PAYLOAD_BYTES_ENCODING = "http.payload.bytes.encoding";
    public static final String HTTP_BATCH_ENABLED = "http.batch.enabled";
    public static final String HTTP_BATCH_MAX_RECORDS = "http.batch.max.records";
    public static final String HTTP_BATCH_ENVELOPE = "http.batch.envelope";
    public static final String HTTP_BATCH_ISOLATE = "http.batch.isolate.failures";

    /* ------------------------------------------------------------------ 认证与 TLS */

    public static final String HTTP_AUTH_TYPE = "http.auth.type";
    public static final String HTTP_AUTH_USERNAME = "http.auth.username";
    public static final String HTTP_AUTH_PASSWORD = "http.auth.password";
    public static final String HTTP_AUTH_TOKEN = "http.auth.token";
    public static final String TLS_TRUSTSTORE_PATH = "http.tls.truststore.path";
    public static final String TLS_TRUSTSTORE_PASSWORD = "http.tls.truststore.password";
    public static final String TLS_TRUSTSTORE_TYPE = "http.tls.truststore.type";
    public static final String TLS_INSECURE = "http.tls.insecure";

    /* ------------------------------------------------------------------ 行为与观测 */

    public static final String BEHAVIOR_ON_ERROR = "behavior.on.error";
    /**
     * 记录级失败的默认行为。
     *
     * <p>配了 DLQ 就该逐条上报：{@code fail} 把异常抛回 {@code put()}，KIP-610 之后
     * Connect 不再用 {@code errors.tolerance=all} 兜这类异常，结果是一条 4xx 让整个
     * task 退出、offset 不提交、重启后反复重放同一条坏记录。没有 reporter 时
     * {@code report} 仍会退回抛异常，所以这个默认值不会让没配 DLQ 的环境悄悄丢数据。
     */
    public static final String BEHAVIOR_ON_ERROR_DEFAULT = "report";
    public static final String HTTP_STATS_INTERVAL_MS = "http.stats.interval.ms";

    private static final ConfigDef DEFINITION = define();

    private final UrlTemplate urlTemplate;
    private final PayloadFormat payloadFormat;
    private final boolean bytesAsBase64;
    private final AuthType authType;
    private final Behavior behavior;
    private final List<String> warnings;

    public HttpSinkConfig(final Map<String, ?> props) {
        super(DEFINITION, props);
        final Problems problems = new Problems();
        final List<String> notes = new ArrayList<>(headerWarnings());

        this.urlTemplate = compileUrl(problems);
        this.authType = resolveAuth(problems);
        this.payloadFormat = PayloadFormat.of(getString(HTTP_PAYLOAD_FORMAT));
        this.bytesAsBase64 = "base64".equalsIgnoreCase(getString(HTTP_PAYLOAD_BYTES_ENCODING));
        this.behavior = Behavior.of(getString(BEHAVIOR_ON_ERROR));

        checkSemantics(problems, notes);
        this.warnings = List.copyOf(notes);
        if (!problems.isEmpty()) {
            throw new ProblemsException(problems.message(), problems.byKey());
        }
    }

    private static ConfigDef define() {
        final ConfigDef def = new ConfigDef();

        def.define(HTTP_URL, Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                Importance.HIGH,
                "目标地址，必须是 http/https。支持 ${topic} 占位符，按记录所属 topic 路由到不同端点。");

        def.define(HTTP_METHOD, Type.STRING, "POST", ValidString.in("POST", "PUT", "PATCH"),
                Importance.MEDIUM, "HTTP 方法。");

        def.define(HTTP_PROTOCOL, Type.STRING, "HTTP_1_1", ValidString.in("HTTP_1_1", "HTTP_2"),
                Importance.LOW,
                "HTTP 版本。默认 HTTP_1_1：HTTP_2 对明文（h2c）要求服务端支持，升级失败会直接连不上。");

        def.define(HTTP_CONTENT_TYPE, Type.STRING, "application/json", Importance.MEDIUM,
                "Content-Type 请求头。");

        def.define(HTTP_USER_AGENT, Type.STRING, "kafka-connect-http-sink/" + Version.VERSION,
                Importance.LOW, "User-Agent 请求头。");

        def.define(HTTP_COMPRESSION, Type.STRING, "none", ValidString.in("none", "gzip"),
                Importance.LOW, "请求体压缩方式。gzip 要求下游支持 Content-Encoding: gzip。");

        def.define(HTTP_CONNECT_TIMEOUT_MS, Type.INT, 5_000, Range.atLeast(1), Importance.MEDIUM,
                "建立 TCP 连接的超时。");

        def.define(HTTP_REQUEST_TIMEOUT_MS, Type.INT, 30_000, Range.atLeast(1), Importance.MEDIUM,
                "单次请求（含等待响应）的超时，由插件自己的重试循环约束。");

        def.define(HTTP_DELIVERY_BUDGET_MS, Type.INT, 60_000, Range.atLeast(0), Importance.HIGH,
                "单次 put() 的总时间预算，0 表示不限。必须明显小于 consumer.max.poll.interval.ms，"
                        + "否则一次下游变慢就会触发 rebalance 并放大重复投递。");

        def.define(HTTP_RETRY_MAX, Type.INT, 8, Range.atLeast(0), Importance.HIGH,
                "单条（批）记录在插件内最多重试几次。框架层的 errors.retry.* 仍作为兜底。");

        def.define(HTTP_RETRY_BACKOFF_MS, Type.INT, 500, Range.atLeast(0), Importance.MEDIUM,
                "首次退避时长，之后按指数增长。");

        def.define(HTTP_RETRY_BACKOFF_MAX_MS, Type.INT, 30_000, Range.atLeast(0), Importance.MEDIUM,
                "退避上限，同时约束下游 Retry-After 的最大值。");

        def.define(HTTP_RETRY_JITTER_MS, Type.INT, 250, Range.atLeast(0), Importance.LOW,
                "退避抖动上限。多个 task 同时被同一个下游拒绝时，抖动能避免它们一起重试。");

        def.define(HTTP_METADATA_HEADERS, Type.BOOLEAN, true, Importance.MEDIUM,
                "是否把源坐标（topic/partition/offset/timestamp/key）与幂等键写进请求头。");

        def.define(HTTP_HEADER_FORWARD, Type.LIST, Collections.emptyList(), Importance.LOW,
                "把哪些 Kafka 记录头透传成请求头（前缀 x-kafka-header-），* 表示全部。");

        def.define(HTTP_PAYLOAD_FORMAT, Type.STRING, PayloadFormat.AUTO.configValue(),
                ValidString.in(PayloadFormat.NAMES), Importance.HIGH,
                "请求体渲染方式：auto 按值类型分派；json 一律序列化；text 一律当纯文本。");

        def.define(HTTP_PAYLOAD_BYTES_ENCODING, Type.STRING, "base64", ValidString.in("base64", "utf8"),
                Importance.LOW, "byte[] 值的编码方式。");

        def.define(HTTP_BATCH_ENABLED, Type.BOOLEAN, false, Importance.MEDIUM,
                "是否把多条记录拼成一个 JSON 数组一次发出。");

        def.define(HTTP_BATCH_MAX_RECORDS, Type.INT, 100, Range.atLeast(1), Importance.MEDIUM,
                "单批最多包含的记录数。");

        def.define(HTTP_BATCH_ENVELOPE, Type.BOOLEAN, false, Importance.LOW,
                "批量模式下是否把每条记录包成 {metadata, value}，以便下游按坐标做幂等。");

        def.define(HTTP_BATCH_ISOLATE, Type.BOOLEAN, true, Importance.LOW,
                "批量请求失败且属于记录级问题时，是否拆成单条重投以定位坏记录（可能产生重复投递）。");

        def.define(HTTP_AUTH_TYPE, Type.STRING, "none", ValidString.in("none", "basic", "bearer"),
                Importance.MEDIUM, "认证方式。");

        def.define(HTTP_AUTH_USERNAME, Type.STRING, null, Importance.MEDIUM, "basic 认证的用户名。");
        def.define(HTTP_AUTH_PASSWORD, Type.PASSWORD, null, Importance.MEDIUM, "basic 认证的密码。");
        def.define(HTTP_AUTH_TOKEN, Type.PASSWORD, null, Importance.MEDIUM, "bearer 认证的令牌。");

        def.define(TLS_TRUSTSTORE_PATH, Type.STRING, null, Importance.LOW,
                "信任库路径，用于校验自签证书的服务端。");
        def.define(TLS_TRUSTSTORE_PASSWORD, Type.PASSWORD, null, Importance.LOW, "信任库密码。");
        def.define(TLS_TRUSTSTORE_TYPE, Type.STRING, "JKS", ValidString.in("JKS", "PKCS12"),
                Importance.LOW, "信任库类型。");
        def.define(TLS_INSECURE, Type.BOOLEAN, false, Importance.LOW,
                "跳过服务端证书校验，仅限联调环境。");

        def.define(BEHAVIOR_ON_ERROR, Type.STRING, BEHAVIOR_ON_ERROR_DEFAULT,
                ValidString.in("fail", "report", "skip"), Importance.HIGH,
                "记录级失败的处理方式（默认 report）：report 交给 ErrantRecordReporter 逐条进 DLQ 后继续；"
                        + "fail 抛给框架（KIP-610 起 errors.tolerance=all 不兜 put() 异常，会让任务退出）；"
                        + "skip 直接丢弃（会丢数据）。");

        def.define(HTTP_STATS_INTERVAL_MS, Type.INT, 60_000, Range.atLeast(0), Importance.LOW,
                "投递汇总日志的间隔，0 表示关闭。");

        return def;
    }

    /* ------------------------------------------------------------------ 校验 */

    /**
     * 语义校验：ConfigDef 表达不了的跨字段约束。
     *
     * <p>返回 {@code key → 错误信息} 而不是直接抛第一条：一次把问题说完，省一轮改配置重启。
     *
     * @param props 原始配置（未解析），供 {@link HttpSinkConnector#validate} 复用
     */
    public static Map<String, List<String>> problems(final Map<String, ?> props) {
        try {
            new HttpSinkConfig(props);
            return Map.of();
        } catch (final ProblemsException e) {
            return e.byKey();
        } catch (final ConfigException e) {
            // 类型/必填这一层由 ConfigDef.validate 报，这里不重复
            return Map.of();
        }
    }

    private void checkSemantics(final Problems problems, final List<String> notes) {
        if (getInt(HTTP_RETRY_BACKOFF_MAX_MS) < getInt(HTTP_RETRY_BACKOFF_MS)) {
            problems.add(HTTP_RETRY_BACKOFF_MAX_MS, "退避上限不能小于首次退避");
        }
        final int budget = getInt(HTTP_DELIVERY_BUDGET_MS);
        if (budget > 0 && budget < getInt(HTTP_REQUEST_TIMEOUT_MS)) {
            notes.add("http.delivery.budget.ms(" + budget + ") 小于 http.request.timeout.ms("
                    + getInt(HTTP_REQUEST_TIMEOUT_MS) + ")，单次请求就可能吃掉整个预算");
        }
        if (getBoolean(HTTP_BATCH_ENABLED)) {
            if (payloadFormat == PayloadFormat.TEXT) {
                problems.add(HTTP_PAYLOAD_FORMAT,
                        "批量模式会把多条记录拼成 JSON 数组，text 模式产出的纯文本无法组成合法数组");
            }
            if (!getBoolean(HTTP_METADATA_HEADERS)) {
                notes.add("批量模式下不发源坐标头；需要幂等请打开 http.batch.envelope");
            }
        } else if (getBoolean(HTTP_BATCH_ENVELOPE)) {
            notes.add("http.batch.envelope=true 只在批量模式下有意义");
        }
        if (getBoolean(HTTP_BATCH_ENABLED) && getBoolean(HTTP_BATCH_ISOLATE)) {
            notes.add("http.batch.isolate.failures=true：批量失败后会拆单重投，下游需能接受重复投递");
        }
        if (authType == AuthType.NONE
                && (getPassword(HTTP_AUTH_PASSWORD) != null || getPassword(HTTP_AUTH_TOKEN) != null)) {
            notes.add("配了 http.auth.password/token 但 http.auth.type=none，认证不会生效");
        }
        if (getBoolean(TLS_INSECURE)) {
            notes.add("http.tls.insecure=true：跳过服务端证书校验，仅限联调");
        }
        if (getString(HTTP_COMPRESSION).equals("gzip") && !getString(HTTP_CONTENT_TYPE).contains("json")) {
            notes.add("开启 gzip 但 Content-Type 不是 JSON，确认下游能解压");
        }
    }

    private UrlTemplate compileUrl(final Problems problems) {
        try {
            return UrlTemplate.compile(getString(HTTP_URL));
        } catch (final IllegalArgumentException e) {
            problems.add(HTTP_URL, e.getMessage());
            return null;
        }
    }

    private AuthType resolveAuth(final Problems problems) {
        final AuthType type = AuthType.of(getString(HTTP_AUTH_TYPE));
        switch (type) {
            case BASIC -> {
                final String user = getString(HTTP_AUTH_USERNAME);
                if (user == null || user.isBlank()) {
                    problems.add(HTTP_AUTH_USERNAME, "http.auth.type=basic 时用户名必填");
                }
                if (isEmpty(getPassword(HTTP_AUTH_PASSWORD))) {
                    problems.add(HTTP_AUTH_PASSWORD, "http.auth.type=basic 时密码必填");
                }
            }
            case BEARER -> {
                if (isEmpty(getPassword(HTTP_AUTH_TOKEN))) {
                    problems.add(HTTP_AUTH_TOKEN, "http.auth.type=bearer 时令牌必填");
                }
            }
            case NONE -> {
                // 无需额外字段
            }
        }
        return type;
    }

    private List<String> headerWarnings() {
        final List<String> result = new ArrayList<>();
        for (final String key : originals().keySet()) {
            if (!key.regionMatches(true, 0, HTTP_HEADER_PREFIX, 0, HTTP_HEADER_PREFIX.length())) {
                continue;
            }
            final String name = key.substring(HTTP_HEADER_PREFIX.length());
            if (name.isEmpty() || !RequestHeaders.isToken(name)) {
                result.add("请求头名不合法，将不会被发送: " + key);
            } else if (RequestHeaders.isReserved(name)) {
                result.add("请求头 " + name + " 由客户端或安全策略掌控，已忽略；请改用 http.content.type / http.auth.*");
            }
        }
        for (final String name : getList(HTTP_HEADER_FORWARD)) {
            if (!RequestHeaders.FORWARD_ALL.equals(name) && !RequestHeaders.isToken(name)) {
                result.add("http.header.forward 里的 " + name + " 不是合法的头名");
            }
        }
        return result;
    }

    private static boolean isEmpty(final org.apache.kafka.common.config.types.Password value) {
        return value == null || value.value() == null || value.value().isEmpty();
    }

    /* ------------------------------------------------------------------ 访问器 */

    public UrlTemplate urlTemplate() {
        return urlTemplate;
    }

    public PayloadFormat payloadFormat() {
        return payloadFormat;
    }

    public boolean bytesAsBase64() {
        return bytesAsBase64;
    }

    public AuthType authType() {
        return authType;
    }

    public Behavior behavior() {
        return behavior;
    }

    /** 启动时该让运维看见的提醒（不影响启动） */
    public List<String> warnings() {
        return warnings;
    }

    public String basicUser() {
        return getString(HTTP_AUTH_USERNAME);
    }

    public char[] basicPassword() {
        return passwordChars(HTTP_AUTH_PASSWORD);
    }

    public char[] bearerToken() {
        return passwordChars(HTTP_AUTH_TOKEN);
    }

    private char[] passwordChars(final String key) {
        final org.apache.kafka.common.config.types.Password value = getPassword(key);
        return value == null ? null : value.value().toCharArray();
    }

    public HttpClient.Version httpVersion() {
        return "HTTP_2".equals(getString(HTTP_PROTOCOL)) ? HttpClient.Version.HTTP_2 : HttpClient.Version.HTTP_1_1;
    }

    public boolean gzip() {
        return "gzip".equals(getString(HTTP_COMPRESSION));
    }

    /**
     * {@code http.headers.*} 收集到的自定义请求头。
     *
     * <p>非法名与保留名在这里被静默跳过，原因在 {@link #warnings()} 里说明。
     */
    public Map<String, String> customHeaders() {
        final Map<String, String> headers = new LinkedHashMap<>();
        for (final Map.Entry<String, Object> entry : originals().entrySet()) {
            final String key = entry.getKey();
            if (!key.regionMatches(true, 0, HTTP_HEADER_PREFIX, 0, HTTP_HEADER_PREFIX.length())) {
                continue;
            }
            final String name = key.substring(HTTP_HEADER_PREFIX.length());
            if (name.isEmpty() || !RequestHeaders.isToken(name) || RequestHeaders.isReserved(name)) {
                continue;
            }
            final String value = entry.getValue() == null ? "" : String.valueOf(entry.getValue());
            headers.put(name, RequestHeaders.sanitize(value));
        }
        return headers;
    }

    /** 不打印任何密钥的配置摘要，供启动日志使用 */
    public String summary() {
        return "url=" + urlTemplate.template()
                + " method=" + getString(HTTP_METHOD)
                + " protocol=" + getString(HTTP_PROTOCOL)
                + " format=" + payloadFormat.configValue()
                + " batch=" + getBoolean(HTTP_BATCH_ENABLED)
                + " retry=" + getInt(HTTP_RETRY_MAX) + "x/" + getInt(HTTP_RETRY_BACKOFF_MS) + "ms"
                + " budget=" + getInt(HTTP_DELIVERY_BUDGET_MS) + "ms"
                + " auth=" + authType.name().toLowerCase(Locale.ROOT)
                + " behavior=" + behavior.configValue();
    }

    /* ------------------------------------------------------------------ 枚举 */

    public enum AuthType {
        NONE, BASIC, BEARER;

        static AuthType of(final String raw) {
            return switch (raw == null ? "none" : raw.toLowerCase(Locale.ROOT)) {
                case "basic" -> BASIC;
                case "bearer" -> BEARER;
                default -> NONE;
            };
        }
    }

    public enum Behavior {
        /** 抛给框架：由 errors.tolerance 决定进 DLQ 还是让任务失败 */
        FAIL("fail"),
        /** 交给 ErrantRecordReporter 逐条进 DLQ，任务继续跑 */
        REPORT("report"),
        /** 直接丢弃，会丢数据 */
        SKIP("skip");

        private final String configValue;

        Behavior(final String configValue) {
            this.configValue = configValue;
        }

        public String configValue() {
            return configValue;
        }

        static Behavior of(final String raw) {
            return switch (raw == null ? BEHAVIOR_ON_ERROR_DEFAULT : raw.toLowerCase(Locale.ROOT)) {
                case "report" -> REPORT;
                case "skip" -> SKIP;
                default -> FAIL;
            };
        }
    }

    /* ------------------------------------------------------------------ 跨字段问题容器 */

    /**
     * 跨字段校验的问题收集器。
     *
     * <p>先收集再抛，而不是遇到第一条就抛：一次把问题说完，省一轮"改配置 → 重启 → 再报错"。
     */
    private static final class Problems {
        private final Map<String, List<String>> byKey = new LinkedHashMap<>();

        void add(final String key, final String message) {
            byKey.computeIfAbsent(key, ignored -> new ArrayList<>()).add(message);
        }

        boolean isEmpty() {
            return byKey.isEmpty();
        }

        Map<String, List<String>> byKey() {
            return Map.copyOf(byKey);
        }

        String message() {
            final List<String> lines = new ArrayList<>();
            byKey.forEach((key, messages) -> messages.forEach(message -> lines.add(key + ": " + message)));
            return String.join("; ", lines);
        }
    }

    /**
     * 承载"按字段分组"的校验错误。
     *
     * <p>做成异常的子类只是为了让 {@link HttpSinkConnector#validate} 能把错误挂到具体输入框上，
     * 消息本身仍然是一句能直接给人看的话。
     */
    public static final class ProblemsException extends ConfigException {
        private static final long serialVersionUID = 1L;

        private final Map<String, List<String>> byKey;

        ProblemsException(final String message, final Map<String, List<String>> byKey) {
            super(message);
            this.byKey = byKey;
        }

        public Map<String, List<String>> byKey() {
            return byKey;
        }
    }

    /** 仅用于单测与日志断言：允许读取全部键名 */
    public static Set<String> definedKeys() {
        return DEFINITION.names();
    }

    public static ConfigDef configDef() {
        return DEFINITION;
    }
}
