package rebue.kafka.httpsink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import rebue.kafka.httpsink.json.PayloadFormat;

class HttpSinkConfigTest {

    @Test
    void minimalConfigUsesSafeDefaults() {
        final HttpSinkConfig config = Configs.of();
        assertEquals(PayloadFormat.AUTO, config.payloadFormat());
        assertEquals(HttpSinkConfig.AuthType.NONE, config.authType());
        assertEquals(HttpSinkConfig.Behavior.FAIL, config.behavior());
        assertFalse(config.gzip());
        assertTrue(config.warnings().isEmpty(), config.warnings().toString());
    }

    @Test
    void urlIsRequired() {
        final ConfigException error = assertThrows(ConfigException.class,
                () -> new HttpSinkConfig(Map.of("http.method", "POST")));
        assertTrue(error.getMessage().contains("http.url"));
    }

    /** 这类问题以前要等 task 起来才会以 NPE 暴露，并且被健康检查反复重启 */
    @Test
    void basicAuthRequiresPassword() {
        final HttpSinkConfig.ProblemsException error = assertThrows(HttpSinkConfig.ProblemsException.class,
                () -> Configs.of(HttpSinkConfig.HTTP_AUTH_TYPE, "basic",
                        HttpSinkConfig.HTTP_AUTH_USERNAME, "u"));
        assertTrue(error.byKey().containsKey(HttpSinkConfig.HTTP_AUTH_PASSWORD));
    }

    @Test
    void bearerAuthRequiresToken() {
        final HttpSinkConfig.ProblemsException error = assertThrows(HttpSinkConfig.ProblemsException.class,
                () -> Configs.of(HttpSinkConfig.HTTP_AUTH_TYPE, "bearer"));
        assertTrue(error.byKey().containsKey(HttpSinkConfig.HTTP_AUTH_TOKEN));
    }

    /** text 模式产出的是纯文本，拼不成 JSON 数组，必须配置期就拒绝 */
    @Test
    void batchRejectsTextPayload() {
        final HttpSinkConfig.ProblemsException error = assertThrows(HttpSinkConfig.ProblemsException.class,
                () -> Configs.of(HttpSinkConfig.HTTP_BATCH_ENABLED, "true",
                        HttpSinkConfig.HTTP_PAYLOAD_FORMAT, "text"));
        assertTrue(error.byKey().containsKey(HttpSinkConfig.HTTP_PAYLOAD_FORMAT));
    }

    @Test
    void retryBackoffMaxMustNotBeBelowInitial() {
        final HttpSinkConfig.ProblemsException error = assertThrows(HttpSinkConfig.ProblemsException.class,
                () -> Configs.of(HttpSinkConfig.HTTP_RETRY_BACKOFF_MS, "5000",
                        HttpSinkConfig.HTTP_RETRY_BACKOFF_MAX_MS, "1000"));
        assertTrue(error.byKey().containsKey(HttpSinkConfig.HTTP_RETRY_BACKOFF_MAX_MS));
    }

    /** 供 Connector.validate() 复用：结果按字段归位，页面才能标到具体输入框 */
    @Test
    void problemsAreGroupedByConfigKey() {
        final Map<String, java.util.List<String>> problems = HttpSinkConfig.problems(
                Configs.map(HttpSinkConfig.HTTP_URL, "ftp://example.com/x"));
        assertTrue(problems.containsKey(HttpSinkConfig.HTTP_URL));
    }

    @Test
    void problemsReturnsEmptyForValidConfig() {
        assertTrue(HttpSinkConfig.problems(Configs.map()).isEmpty());
    }

    @Test
    void warnsAboutInsecureTlsAndIgnoredCredentials() {
        final HttpSinkConfig config = Configs.of(
                HttpSinkConfig.TLS_INSECURE, "true",
                HttpSinkConfig.HTTP_AUTH_PASSWORD, "secret");
        assertEquals(2, config.warnings().size(), config.warnings().toString());
    }

    @Test
    void collectsCustomHeadersAndSkipsReservedOnes() {
        final HttpSinkConfig config = Configs.of(
                "http.headers.X-Tenant", "acme",
                "http.headers.Authorization", "should-be-ignored");
        final Map<String, String> headers = config.customHeaders();
        assertEquals("acme", headers.get("X-Tenant"));
        assertFalse(headers.containsKey("Authorization"));
    }

    /** 启动日志不能打印密钥 */
    @Test
    void summaryNeverLeaksSecrets() {
        final HttpSinkConfig config = Configs.of(
                HttpSinkConfig.HTTP_AUTH_TYPE, "basic",
                HttpSinkConfig.HTTP_AUTH_USERNAME, "user",
                HttpSinkConfig.HTTP_AUTH_PASSWORD, "p@ssw0rd",
                HttpSinkConfig.HTTP_AUTH_TOKEN, "t0ken");
        final String summary = config.summary();
        assertFalse(summary.contains("p@ssw0rd"));
        assertFalse(summary.contains("t0ken"));
        assertTrue(summary.contains("auth=basic"));
    }

    @Test
    void acceptsTopicPlaceholderAndProtocolChoice() {
        final HttpSinkConfig config = Configs.of(
                HttpSinkConfig.HTTP_URL, "https://example.com/sync/${topic}",
                HttpSinkConfig.HTTP_PROTOCOL, "HTTP_2");
        assertTrue(config.urlTemplate().templated());
        assertEquals(java.net.http.HttpClient.Version.HTTP_2, config.httpVersion());
    }
}
