package rebue.kafka.httpsink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;

import org.junit.jupiter.api.Test;

import rebue.kafka.httpsink.route.UrlTemplate;

class UrlTemplateTest {

    @Test
    void fixedUrlIsResolvedOnce() {
        final UrlTemplate template = UrlTemplate.compile("https://example.com/sync/orders");
        assertFalse(template.templated());
        assertEquals(URI.create("https://example.com/sync/orders"), template.resolve("any-topic"));
        assertEquals(URI.create("https://example.com/sync/orders"), template.resolve("other"));
    }

    @Test
    void topicPlaceholderRoutesPerTopic() {
        final UrlTemplate template = UrlTemplate.compile("http://magic:9999/sync/${topic}");
        assertTrue(template.templated());
        assertEquals(URI.create("http://magic:9999/sync/dtw.public.dtw_polygon"),
                template.resolve("dtw.public.dtw_polygon"));
        assertEquals(URI.create("http://magic:9999/sync/adm.adm.ADM_USER"),
                template.resolve("adm.adm.ADM_USER"));
    }

    /** 带凭据的 URL 会把密码写进日志、异常信息和 Connect 的配置快照 */
    @Test
    void rejectsCredentialsInUrl() {
        final IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> UrlTemplate.compile("http://user:pass@example.com/sync"));
        assertTrue(error.getMessage().contains("http.auth"));
    }

    @Test
    void rejectsNonHttpScheme() {
        assertThrows(IllegalArgumentException.class, () -> UrlTemplate.compile("ftp://example.com/x"));
        assertThrows(IllegalArgumentException.class, () -> UrlTemplate.compile("file:///etc/passwd"));
    }

    @Test
    void rejectsUnknownPlaceholder() {
        final IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> UrlTemplate.compile("http://example.com/${env}/sync"));
        assertTrue(error.getMessage().contains("${env}"));
    }

    @Test
    void rejectsBlankAndHostless() {
        assertThrows(IllegalArgumentException.class, () -> UrlTemplate.compile("  "));
        assertThrows(IllegalArgumentException.class, () -> UrlTemplate.compile("http:///sync"));
    }
}
