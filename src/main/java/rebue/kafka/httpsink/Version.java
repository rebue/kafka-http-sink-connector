package rebue.kafka.httpsink;

/**
 * 插件版本。
 *
 * <p>2.x 是一次不兼容的重写：去掉了 1.x 里为兼容 wheel-kafka 3.5.x 而保留的
 * {@code rebue.wheel.kafka.httpsink} 包名与 {@code http.api.url} 等旧键。
 * 迁移方式见仓库 README 的「从 3.5.x 迁移」一节。
 */
public final class Version {
    public static final String VERSION = "2.0.0";

    private Version() {
    }
}
