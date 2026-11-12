package rebue.kafka.httpsink;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * 用线上（lhlj-prod / kafka01）两个真实连接器的配置做回归。
 *
 * <p>这些连接器还在跑老实现，jar 替换后 worker 一旦重启就会切到新代码。所以在替换前
 * 先把「线上原样的配置」喂给新的 {@link HttpSinkConnector}，确认能起 task、URL 解析正确。
 * 配置里带了不少新实现不认识的键（{@code errors.*}、{@code topics}、{@code name}），
 * 这里同时验证未知键不会导致启动失败。
 */
class ProdConfigCompatTest {

    /** dtw-to-dtwrs-dtw-region-sink 的线上配置（原样拷贝，仅格式化） */
    private static Map<String, String> regionSink() {
        final Map<String, String> props = base();
        props.put("http.api.url", "http://magic:9999/sync/dtw-to-dtwrs-dtw-region");
        props.put("topics", "dtw.public.dtw_polygon");
        props.put("name", "dtw-to-dtwrs-dtw-region-sink");
        return props;
    }

    /** adm-to-dtwrs-dtw-person-sink 的线上配置（原样拷贝，仅格式化） */
    private static Map<String, String> personSink() {
        final Map<String, String> props = base();
        props.put("http.api.url", "http://magic:9999/sync/adm-to-dtwrs-dtw-person");
        props.put("topics", "adm.adm.ADM_USER");
        props.put("name", "adm-to-dtwrs-dtw-person-sink");
        return props;
    }

    /** 两个连接器共有的部分：dbz 注入的 DLQ 与重试配置 */
    private static Map<String, String> base() {
        final Map<String, String> props = new LinkedHashMap<>();
        props.put("connector.class", "rebue.wheel.kafka.httpsink.HttpSinkConnector");
        props.put("tasks.max", "1");
        props.put("errors.tolerance", "all");
        props.put("errors.log.enable", "true");
        props.put("errors.log.include.messages", "false");
        props.put("errors.retry.timeout", "300000");
        props.put("errors.retry.delay.max.ms", "30000");
        props.put("errors.deadletterqueue.topic.name", "dbz-connect-dlq");
        props.put("errors.deadletterqueue.topic.replication.factor", "3");
        props.put("errors.deadletterqueue.context.headers.enable", "true");
        // 与 Confluent 同名的重试参数，新实现会真正读取
        props.put("max.retries", "10");
        props.put("retry.backoff.ms", "3000");
        return props;
    }

    @Test
    void regionSinkStartsAndResolvesUrl() {
        assertCompat(regionSink(), "http://magic:9999/sync/dtw-to-dtwrs-dtw-region");
    }

    @Test
    void personSinkStartsAndResolvesUrl() {
        assertCompat(personSink(), "http://magic:9999/sync/adm-to-dtwrs-dtw-person");
    }

    /** 线上配置全部只填 http.api.url，必须回落到 http.url 语义 */
    @Test
    void legacyHttpApiUrlIsHonoured() {
        final HttpSinkConfig config = new HttpSinkConfig(regionSink());
        assertEquals("http://magic:9999/sync/dtw-to-dtwrs-dtw-region", config.urlTemplate());
    }

    private static void assertCompat(final Map<String, String> props, final String expectedUrl) {
        final HttpSinkConnector connector = new HttpSinkConnector();
        assertDoesNotThrow(() -> connector.start(props));

        final int maxTasks = Integer.parseInt(props.get("tasks.max"));
        final List<Map<String, String>> taskConfigs = connector.taskConfigs(maxTasks);
        assertEquals(maxTasks, taskConfigs.size());

        final HttpSinkTask task = new HttpSinkTask();
        assertDoesNotThrow(() -> task.start(taskConfigs.get(0)));
        // 起得来只是第一关：URL 解析错了会在第一条记录时才炸
        final HttpSinkConfig config = new HttpSinkConfig(taskConfigs.get(0));
        assertEquals(expectedUrl, config.urlFor(props.get("topics")));
        assertEquals(10, config.maxRetries());
        assertDoesNotThrow(task::stop);
        connector.stop();
    }

    /** 老配置没填 http.url，校验器不能因此报错 */
    @Test
    void validateAcceptsLegacyOnlyConfig() {
        final HttpSinkConnector connector = new HttpSinkConnector();
        assertTrue(connector.validate(regionSink()).configValues().stream()
                .allMatch(value -> value.errorMessages().isEmpty()));
    }
}
