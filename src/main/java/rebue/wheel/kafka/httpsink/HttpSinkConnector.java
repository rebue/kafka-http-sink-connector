package rebue.wheel.kafka.httpsink;

/**
 * 老包名的兼容入口：{@code rebue.wheel.kafka.httpsink.HttpSinkConnector}。
 *
 * <p>作用是让已部署的连接器**只换 jar、不改配置**：{@code connector.class} 仍然指向这个
 * 全限定名，行为由新实现接管。
 *
 * <p>注意：部署时必须先把老的 {@code http-sink-connector} jar 从 {@code plugin.path} 移除，
 * 否则同一个类名会在两个 jar 里出现，Connect 扫到哪个不确定。
 */
public class HttpSinkConnector extends rebue.kafka.httpsink.HttpSinkConnector {
}
