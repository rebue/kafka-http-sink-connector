package rebue.kafka.httpsink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import rebue.kafka.httpsink.support.RequestHeaders;

class RequestHeadersTest {

    private static SinkRecord record() {
        return new SinkRecord("orders", 2, Schema.STRING_SCHEMA, "k-1",
                Schema.STRING_SCHEMA, "{}", 42L);
    }

    @Test
    void metadataHeadersCarryIdempotencyKey() {
        final Map<String, String> headers = RequestHeaders.of(Configs.of()).forRecord(record());
        assertEquals("orders", headers.get("X-Kafka-Topic"));
        assertEquals("2", headers.get("X-Kafka-Partition"));
        assertEquals("42", headers.get("X-Kafka-Offset"));
        assertEquals("k-1", headers.get("X-Kafka-Key"));
        // 下游只需要这一个头就能去重，不必自己拼三段
        assertEquals("orders-2-42", headers.get(RequestHeaders.IDEMPOTENCY_KEY));
        assertEquals("application/json", headers.get("Content-Type"));
    }

    @Test
    void metadataCanBeDisabled() {
        final Map<String, String> headers = RequestHeaders
                .of(Configs.of(HttpSinkConfig.HTTP_METADATA_HEADERS, "false")).forRecord(record());
        assertNull(headers.get("X-Kafka-Topic"));
        assertNull(headers.get(RequestHeaders.IDEMPOTENCY_KEY));
    }

    @Test
    void forwardsOnlyListedRecordHeaders() {
        final SinkRecord withHeaders = new SinkRecord("orders", 0, null, null,
                Schema.STRING_SCHEMA, "{}", 1L, null, null,
                new ConnectHeaders().addString("tenant", "acme").addString("trace", "t-1"));
        final Map<String, String> headers = RequestHeaders
                .of(Configs.of(HttpSinkConfig.HTTP_HEADER_FORWARD, "tenant")).forRecord(withHeaders);
        assertEquals("acme", headers.get("x-kafka-header-tenant"));
        assertNull(headers.get("x-kafka-header-trace"));
    }

    @Test
    void forwardAllSkipsConnectErrorContext() {
        final SinkRecord withHeaders = new SinkRecord("orders", 0, null, null,
                Schema.STRING_SCHEMA, "{}", 1L, null, null,
                new ConnectHeaders().addString("a", "1").addString("__connect.errors.topic", "secret"));
        final Map<String, String> headers = RequestHeaders
                .of(Configs.of(HttpSinkConfig.HTTP_HEADER_FORWARD, "*")).forRecord(withHeaders);
        assertEquals("1", headers.get("x-kafka-header-a"));
        assertNull(headers.get("x-kafka-header-__connect.errors.topic"));
    }

    /** 头注入防线：上游任意字节里的 CR/LF 不能进到我们的请求头 */
    @Test
    void stripsControlCharactersFromForwardedValues() {
        final SinkRecord withHeaders = new SinkRecord("orders", 0, null, null,
                Schema.STRING_SCHEMA, "{}", 1L, null, null,
                new ConnectHeaders().addString("evil", "a\r\nX-Injected: 1"));
        final Map<String, String> headers = RequestHeaders
                .of(Configs.of(HttpSinkConfig.HTTP_HEADER_FORWARD, "*")).forRecord(withHeaders);
        assertEquals("a  X-Injected: 1", headers.get("x-kafka-header-evil"));
        assertNull(headers.get("X-Injected"));
    }

    @Test
    void customHeadersAreApplied() {
        final Map<String, String> headers = RequestHeaders
                .of(Configs.of("http.headers.X-Tenant", "acme")).forRecord(record());
        assertEquals("acme", headers.get("X-Tenant"));
    }

    /** Content-Type 归 http.content.type 管，不允许用 http.headers.* 覆盖 */
    @Test
    void reservedCustomHeadersAreIgnored() {
        final HttpSinkConfig config = Configs.of("http.headers.content-type", "text/plain");
        assertEquals("application/json",
                RequestHeaders.of(config).forRecord(record()).get("Content-Type"));
        assertTrue(config.warnings().stream().anyMatch(w -> w.contains("content-type")));
    }

    @Test
    void basicAuthIsEncoded() {
        final Map<String, String> headers = RequestHeaders.of(Configs.of(
                HttpSinkConfig.HTTP_AUTH_TYPE, "basic",
                HttpSinkConfig.HTTP_AUTH_USERNAME, "u",
                HttpSinkConfig.HTTP_AUTH_PASSWORD, "p")).forRecord(record());
        assertEquals("Basic " + Base64.getEncoder().encodeToString("u:p".getBytes(StandardCharsets.UTF_8)),
                headers.get("Authorization"));
    }

    @Test
    void bearerAuthIsApplied() {
        final Map<String, String> headers = RequestHeaders.of(Configs.of(
                HttpSinkConfig.HTTP_AUTH_TYPE, "bearer",
                HttpSinkConfig.HTTP_AUTH_TOKEN, "t-0ken")).forRecord(record());
        assertEquals("Bearer t-0ken", headers.get("Authorization"));
    }

    /** 批量请求跨多条记录，源坐标会失真，所以只带静态头 */
    @Test
    void batchHeadersHaveNoRecordCoordinates() {
        final Map<String, String> headers = RequestHeaders.of(Configs.of()).forBatch();
        assertFalse(headers.containsKey("X-Kafka-Offset"));
        assertEquals("application/json", headers.get("Content-Type"));
    }
}
