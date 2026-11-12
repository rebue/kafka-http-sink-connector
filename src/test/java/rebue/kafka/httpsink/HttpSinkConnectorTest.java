package rebue.kafka.httpsink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;

class HttpSinkConnectorTest {

    @Test
    void reportsVersion() {
        assertEquals(Version.VERSION, new HttpSinkConnector().version());
    }

    @Test
    void taskConfigsHonourTasksMax() {
        final HttpSinkConnector connector = new HttpSinkConnector();
        connector.start(Configs.map());
        assertEquals(1, connector.taskConfigs(1).size());
        assertEquals(4, connector.taskConfigs(4).size());
        // tasks.max 填 0 或负数时，Connect 不应拿到 0 个 task
        assertEquals(1, connector.taskConfigs(0).size());
    }

    @Test
    void validateAcceptsValidConfig() {
        final Config config = new HttpSinkConnector().validate(Configs.map());
        assertTrue(noErrors(config), errors(config));
    }

    /**
     * 跨字段错误必须归位到具体键上。
     *
     * <p>dbz 这类管控面把 errorMessages 标到对应输入框，指向 http.auth.password 才叫"能修改"，
     * 只给一句整体错误就只能靠运维自己猜。
     */
    @Test
    void validateAttachesCrossFieldErrorToItsKey() {
        final Config config = new HttpSinkConnector().validate(Configs.map(
                HttpSinkConfig.HTTP_AUTH_TYPE, "basic",
                HttpSinkConfig.HTTP_AUTH_USERNAME, "u"));

        final ConfigValue password = value(config, HttpSinkConfig.HTTP_AUTH_PASSWORD);
        assertFalse(password.errorMessages().isEmpty(), "密码缺失未被报错");
        assertTrue(password.errorMessages().get(0).contains("密码必填"));
    }

    @Test
    void validateReportsBadUrlScheme() {
        final Config config = new HttpSinkConnector().validate(
                Map.of(HttpSinkConfig.HTTP_URL, "ftp://example.com/x"));
        assertFalse(value(config, HttpSinkConfig.HTTP_URL).errorMessages().isEmpty());
    }

    /** ConfigDef 层的错误（类型、必填、取值范围）也不能被重写后的 validate 吞掉 */
    @Test
    void validateKeepsConfigDefErrors() {
        final Config missingUrl = new HttpSinkConnector().validate(Map.of(HttpSinkConfig.HTTP_METHOD, "POST"));
        assertFalse(value(missingUrl, HttpSinkConfig.HTTP_URL).errorMessages().isEmpty());

        final Config badType = new HttpSinkConnector().validate(Configs.map(
                HttpSinkConfig.HTTP_RETRY_MAX, "not-a-number"));
        assertFalse(badType.configValues().stream().allMatch(v -> v.errorMessages().isEmpty()));
    }

    private static ConfigValue value(final Config config, final String key) {
        return config.configValues().stream()
                .filter(candidate -> candidate.name().equals(key))
                .findFirst()
                .orElseThrow(() -> new AssertionError("没有 " + key + " 这一项"));
    }

    private static boolean noErrors(final Config config) {
        return config.configValues().stream().allMatch(value -> value.errorMessages().isEmpty());
    }

    private static String errors(final Config config) {
        final List<String> all = config.configValues().stream()
                .flatMap(value -> value.errorMessages().stream().map(message -> value.name() + ": " + message))
                .toList();
        return all.toString();
    }
}
