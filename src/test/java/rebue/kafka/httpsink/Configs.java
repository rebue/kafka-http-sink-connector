package rebue.kafka.httpsink;

import java.util.LinkedHashMap;
import java.util.Map;

/** 测试用的配置工厂：默认给一份能启动的最小配置，用例只覆盖自己关心的键 */
final class Configs {
    static final String URL = "http://127.0.0.1:9/sync";

    private Configs() {
    }

    static Map<String, String> map(final String... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("键值必须成对");
        }
        final Map<String, String> props = new LinkedHashMap<>();
        props.put(HttpSinkConfig.HTTP_URL, URL);
        for (int i = 0; i < keyValues.length; i += 2) {
            final String key = keyValues[i];
            final String value = keyValues[i + 1];
            if (value == null) {
                props.remove(key);
            } else {
                props.put(key, value);
            }
        }
        return props;
    }

    static HttpSinkConfig of(final String... keyValues) {
        return new HttpSinkConfig(map(keyValues));
    }
}
