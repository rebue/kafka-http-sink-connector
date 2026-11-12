package rebue.kafka.httpsink.delivery;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;

/**
 * 一次投递（含插件内重试）的最终结论。
 *
 * <p>整个死信链路是否成立，全看这里的三分类：Connect 只认识"成功 / 可重试 / 不可重试"，
 * 它不知道 HTTP 语义。分错的代价不对称 —— 把可重试判成不可重试，一次下游抖动就变成一堆
 * 需要人工处理的死信；把不可重试判成可重试，任务会对着一条永远失败的记录死循环重启。
 */
public final class DeliveryResult {

    public enum Kind {
        /** 2xx：可以提交 offset */
        DELIVERED,
        /** 网络异常、超时、408/425/429/5xx：值得重试，重试用尽后交给框架 */
        RETRYABLE,
        /** 其余 4xx、3xx：重试没有意义，属于记录或配置层面的问题 */
        POISON
    }

    private static final int DETAIL_LIMIT = 512;

    private final Kind kind;
    private final int status;
    private final String detail;
    private final long retryAfterMs;
    private final int attempts;
    private final long elapsedMs;

    private DeliveryResult(final Kind kind, final int status, final String detail,
                           final long retryAfterMs, final int attempts, final long elapsedMs) {
        this.kind = kind;
        this.status = status;
        this.detail = detail == null ? "" : detail;
        this.retryAfterMs = retryAfterMs;
        this.attempts = attempts;
        this.elapsedMs = elapsedMs;
    }

    /* ------------------------------------------------------------------ 工厂 */

    public static DeliveryResult delivered(final int status, final int attempts, final long elapsedMs) {
        return new DeliveryResult(Kind.DELIVERED, status, "", 0, attempts, elapsedMs);
    }

    /**
     * 按 HTTP 状态码分类。
     *
     * @param bodyPreview 响应体摘要，只在失败时保留（成功时的响应体没有排查价值，
     *                    留着只会白占内存、还可能把下游返回的敏感数据带进异常信息）
     */
    public static DeliveryResult fromStatus(final int status, final String bodyPreview,
                                            final long retryAfterMs, final int attempts,
                                            final long elapsedMs) {
        if (status >= 200 && status < 300) {
            return delivered(status, attempts, elapsedMs);
        }
        final Kind kind = isRetriableStatus(status) ? Kind.RETRYABLE : Kind.POISON;
        return new DeliveryResult(kind, status, describeStatus(status, bodyPreview),
                retryAfterMs, attempts, elapsedMs);
    }

    /** 没有拿到响应：连接失败、读超时、TLS 握手失败等，一律可重试 */
    public static DeliveryResult transportFailure(final String reason, final int attempts, final long elapsedMs) {
        return new DeliveryResult(Kind.RETRYABLE, 0, truncate(reason), 0, attempts, elapsedMs);
    }

    static DeliveryResult poison(final String reason, final int status, final long elapsedMs) {
        return new DeliveryResult(Kind.POISON, status, truncate(reason), 0, 1, elapsedMs);
    }

    /** 重试循环收尾时回填"总共试了几次、一共花了多久" */
    public DeliveryResult restated(final int attempts, final long elapsedMs) {
        return new DeliveryResult(kind, status, detail, retryAfterMs, attempts, elapsedMs);
    }

    /** 保留原状态码，只补一句原因（例如"预算不足以再试"），避免把 503 说成"无响应" */
    public DeliveryResult appended(final String note) {
        final String merged = detail.isEmpty() ? note : detail + " · " + note;
        return new DeliveryResult(kind, status, merged, retryAfterMs, attempts, elapsedMs);
    }

    /* ------------------------------------------------------------------ 判定 */

    /**
     * 值得重试的状态码。
     *
     * <p>408（请求超时）、425（Too Early）、429（限流）与 5xx 都是"过一会儿再试可能就好了"；
     * 其余 4xx 是请求本身的问题，重试只会在 DLQ 里堆更多同样的坏记录。
     */
    public static boolean isRetriableStatus(final int status) {
        return status == 408 || status == 425 || status == 429 || (status >= 500 && status <= 599);
    }

    private static String describeStatus(final int status, final String bodyPreview) {
        final StringBuilder sb = new StringBuilder(64);
        sb.append("HTTP ").append(status);
        if (status == 401 || status == 403) {
            sb.append("（认证/授权失败，重试没有意义，检查 http.auth.* 与下游白名单）");
        } else if (status >= 300 && status < 400) {
            sb.append("（重定向不跟随：POST 被 301/302 改写为 GET 会丢掉整个请求语义）");
        }
        if (bodyPreview != null && !bodyPreview.isBlank()) {
            sb.append(" body=").append(truncate(bodyPreview));
        }
        return sb.toString();
    }

    static String truncate(final String text) {
        if (text == null) {
            return "";
        }
        final String oneLine = text.replace('\n', ' ').replace('\r', ' ');
        return oneLine.length() <= DETAIL_LIMIT ? oneLine : oneLine.substring(0, DETAIL_LIMIT) + "...";
    }

    /* ------------------------------------------------------------------ 访问器 */

    public Kind kind() {
        return kind;
    }

    public boolean delivered() {
        return kind == Kind.DELIVERED;
    }

    public boolean retriable() {
        return kind == Kind.RETRYABLE;
    }

    public int status() {
        return status;
    }

    public String detail() {
        return detail;
    }

    long retryAfterMs() {
        return retryAfterMs;
    }

    public int attempts() {
        return attempts;
    }

    public long elapsedMs() {
        return elapsedMs;
    }

    /** 一行摘要，用于日志与异常信息；不含请求体、不含任何凭据 */
    public String describe() {
        if (kind == Kind.DELIVERED) {
            return "HTTP " + status + " 用时 " + elapsedMs + "ms 尝试 " + attempts + " 次";
        }
        return (status == 0 ? "无响应" : "HTTP " + status)
                + (detail.isEmpty() ? "" : " " + detail)
                + " 用时 " + elapsedMs + "ms 尝试 " + attempts + " 次";
    }

    /**
     * 转成 Connect 认识的异常。
     *
     * <p>可重试 → {@link RetriableException}，框架按 {@code errors.retry.*} 重试且**不提交 offset**；
     * 不可重试 → {@link DataException}，在 {@code errors.tolerance=all} 下进死信队列。
     */
    public RuntimeException toException(final String target) {
        final String message = "HTTP 投递失败 " + target + ": " + describe();
        return kind == Kind.RETRYABLE ? new RetriableException(message) : new DataException(message);
    }
}
