package rebue.kafka.httpsink;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/**
 * Kafka Connect HTTP Sink 连接器。
 *
 * <p>连接器本身很薄：校验配置、把配置复制给 task。真正的逻辑在 {@link HttpSinkTask}。
 *
 * <p>{@link #validate(Map)} 被重写，是为了让"页面上的配置校验"和"真正启动时的校验"用同一套
 * 规则 —— 只靠 {@link ConfigDef} 只能查类型和必填，查不出"auth.type=basic 却没给密码"
 * 这类问题，而这类问题恰恰最容易在部署后才暴露成反复 FAILED 的 task。
 */
public class HttpSinkConnector extends SinkConnector {
    private Map<String, String> props = Map.of();

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(final Map<String, String> props) {
        // 只校验不打印：props 里可能有 http.auth.password / http.auth.token
        new HttpSinkConfig(props);
        this.props = Map.copyOf(props);
    }

    /** 动态配置更新时同样要重新校验，否则一份坏配置会在重启 task 时才炸 */
    @Override
    public void reconfigure(final Map<String, String> props) {
        start(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HttpSinkTask.class;
    }

    /**
     * 返回 {@code maxTasks} 份配置，由 Connect 把分区摊给各个 task。
     *
     * <p>多 task 会打乱**跨分区**的顺序（单分区内仍然有序）。需要全局有序就把 {@code tasks.max}
     * 设成 1；这也是默认值。
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
        // 无资源需要释放：HttpClient 与统计都挂在 task 上
    }

    @Override
    public ConfigDef config() {
        return HttpSinkConfig.configDef();
    }

    /**
     * 校验并返回按字段归位的错误。
     *
     * <p>Connect 的 {@code PUT /connector-plugins/{class}/config/validate} 走这里，
     * dbz 之类的管控面据此把红字标在对应输入框上。
     */
    @Override
    public Config validate(final Map<String, String> connectorConfig) {
        final Config base = super.validate(connectorConfig);
        final Map<String, List<String>> problems = HttpSinkConfig.problems(connectorConfig);
        if (problems.isEmpty()) {
            return base;
        }
        final List<ConfigValue> values = new ArrayList<>(base.configValues());
        problems.forEach((key, messages) -> {
            final ConfigValue target = values.stream()
                    .filter(value -> value.name().equals(key))
                    .findFirst()
                    // 动态键（http.headers.*）不在 ConfigDef 里，现造一个承载错误
                    .orElseGet(() -> {
                        final ConfigValue created = new ConfigValue(key);
                        values.add(created);
                        return created;
                    });
            messages.forEach(target::addErrorMessage);
        });
        return new Config(Collections.unmodifiableList(values));
    }
}
