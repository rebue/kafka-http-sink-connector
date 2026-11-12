package rebue.kafka.httpsink.route;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * 目标地址模板。
 *
 * <p>支持一个占位符 {@code ${topic}}，按记录所属 topic 路由到不同端点。这是一个 task 订阅多个
 * topic 时唯一正确的做法 —— 反过来（所有 topic 共用一个地址）会让下游无法区分来源。
 *
 * <p>校验放在**编译期**（连接器启动时）而不是发送时：地址写错属于配置错误，应该在
 * {@code PUT /connectors} 那一刻就被拒绝，而不是等到第一条记录才失败。
 */
public final class UrlTemplate {
    public static final String TOPIC_VAR = "${topic}";

    /** 解析模板时用的占位样本；只要它能构成合法 URI，实际 topic 也能 */
    private static final String SAMPLE = "sample-topic";

    /** 一个 task 同时订阅的 topic 通常个位数，缓存足够且不会成为泄漏点 */
    private static final int CACHE_LIMIT = 64;

    private final String template;
    private final boolean templated;
    private final URI fixed;
    private final Map<String, URI> cache = new LinkedHashMap<>(8, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(final Map.Entry<String, URI> eldest) {
            return size() > CACHE_LIMIT;
        }
    };

    private UrlTemplate(final String template, final boolean templated, final URI fixed) {
        this.template = template;
        this.templated = templated;
        this.fixed = fixed;
    }

    /**
     * @throws IllegalArgumentException 地址不是 http(s)、带用户名密码、或含未知占位符
     */
    public static UrlTemplate compile(final String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("http.url 不能为空");
        }
        final String template = raw.trim();
        rejectUnknownPlaceholders(template);

        final boolean templated = template.contains(TOPIC_VAR);
        final String probe = templated ? template.replace(TOPIC_VAR, SAMPLE) : template;
        final URI uri;
        try {
            uri = new URI(probe);
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("http.url 不是合法的 URL: " + e.getMessage(), e);
        }
        final String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        if (!"http".equals(scheme) && !"https".equals(scheme)) {
            throw new IllegalArgumentException("http.url 只支持 http/https，实际是: " + scheme);
        }
        if (uri.getHost() == null || uri.getHost().isBlank()) {
            throw new IllegalArgumentException("http.url 缺少主机名: " + template);
        }
        if (uri.getUserInfo() != null) {
            // 凭据会跟着地址进日志、进异常信息、进 Connect 的配置快照
            throw new IllegalArgumentException("http.url 里不要带用户名密码，请改用 http.auth.*");
        }
        return new UrlTemplate(template, templated, templated ? null : uri);
    }

    private static void rejectUnknownPlaceholders(final String template) {
        int from = 0;
        while (true) {
            final int start = template.indexOf("${", from);
            if (start < 0) {
                return;
            }
            if (!template.startsWith(TOPIC_VAR, start)) {
                final int end = template.indexOf('}', start);
                final String found = end < 0 ? template.substring(start) : template.substring(start, end + 1);
                throw new IllegalArgumentException("http.url 里只支持 " + TOPIC_VAR + " 占位符，发现 " + found);
            }
            from = start + TOPIC_VAR.length();
        }
    }

    public String template() {
        return template;
    }

    /** 是否按 topic 路由；false 时所有记录打同一个地址 */
    public boolean templated() {
        return templated;
    }

    /** 该地址是否会随 topic 变化 —— 批量投递必须按它分组，否则会把不同端点的记录混成一批 */
    public URI resolve(final String topic) {
        if (!templated) {
            return fixed;
        }
        final String key = topic == null ? "" : topic;
        final URI cached = cache.get(key);
        if (cached != null) {
            return cached;
        }
        final URI uri;
        try {
            uri = new URI(template.replace(TOPIC_VAR, key));
        } catch (final URISyntaxException e) {
            // topic 名本身非法（正常 Kafka 名不会到这里），属于记录级问题
            throw new IllegalArgumentException("topic 无法拼进 http.url: " + key, e);
        }
        cache.put(key, uri);
        return uri;
    }
}
