package rebue.kafka.httpsink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/**
 * Kafka Connect HTTP Sink 连接器。
 *
 * <p>与老实现的差别（每一条都对应一次线上事故面）：
 * <ul>
 *   <li>{@link #taskConfigs(int)} 返回 {@code maxTasks} 份配置，{@code tasks.max} 才真的生效；
 *       老实现只返回 1 份，页面上填多少都只有一个 task。</li>
 *   <li>{@link #start(Map)} 先按 {@link HttpSinkConfig} 全量校验，配置错误在连接器启动时
 *       就 FAILED，而不是等到第一条记录才炸。</li>
 *   <li>不打印原始配置（老实现 {@code log.info(props)} 会把 basic 密码打进日志）。</li>
 * </ul>
 */
public class HttpSinkConnector extends SinkConnector {
    private Map<String, String> props = Map.of();

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(final Map<String, String> props) {
        // 只校验，不打印：props 里可能有 http.auth.password / http.auth.token
        new HttpSinkConfig(props);
        this.props = Map.copyOf(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HttpSinkTask.class;
    }

    /**
     * 返回 {@code maxTasks} 份配置，由 Connect 把分区摊给各个 task。
     *
     * <p>多 task 会打乱**跨分区**的顺序（单分区内仍然有序）。HTTP 投递大多不要求跨分区有序，
     * 需要全局有序时把 {@code tasks.max} 设成 1。
     */
    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        final int count = Math.max(1, maxTasks);
        final List<Map<String, String>> configs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            configs.add(new HashMap<>(props));
        }
        return configs;
    }

    @Override
    public void stop() {
        // 无需释放资源：HttpClient 与线程池都在 task 侧
    }

    @Override
    public ConfigDef config() {
        return HttpSinkConfig.configDef();
    }
}
