package rebue.kafka.httpsink;

/**
 * 插件版本。
 *
 * <p>这一版是不兼容重写（去掉了 wheel-kafka 3.5.x 的 {@code rebue.wheel.kafka.httpsink}
 * 包名与 {@code http.api.url} 等旧键），但从未正式上线，所以版本号仍是 1.0.0。
 */
public final class Version {
    public static final String VERSION = "1.0.0";

    private Version() {
    }
}
