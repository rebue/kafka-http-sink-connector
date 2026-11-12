package rebue.kafka.httpsink.delivery;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rebue.kafka.httpsink.HttpSinkConfig;

/**
 * TLS 上下文。
 *
 * <p>默认返回 {@code null}，表示用 JVM 自带的信任链（系统 CA）。只有配了信任库或显式
 * {@code http.tls.insecure=true} 时才自建。
 */
final class TlsSupport {
    private static final Logger log = LoggerFactory.getLogger(TlsSupport.class);

    private TlsSupport() {
    }

    static SSLContext create(final HttpSinkConfig config) {
        final String truststore = config.getString(HttpSinkConfig.TLS_TRUSTSTORE_PATH);
        final boolean insecure = config.getBoolean(HttpSinkConfig.TLS_INSECURE);
        if (!insecure && (truststore == null || truststore.isBlank())) {
            return null;
        }
        if (insecure) {
            log.warn("http.tls.insecure=true：跳过了服务端证书校验，仅限联调环境使用");
            return trustAll();
        }
        try {
            final KeyStore store = KeyStore.getInstance(config.getString(HttpSinkConfig.TLS_TRUSTSTORE_TYPE));
            final Password password = config.getPassword(HttpSinkConfig.TLS_TRUSTSTORE_PASSWORD);
            try (InputStream input = Files.newInputStream(Path.of(truststore))) {
                store.load(input, password == null ? new char[0] : password.value().toCharArray());
            }
            final TrustManagerFactory factory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factory.init(store);
            final SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, factory.getTrustManagers(), new SecureRandom());
            return context;
        } catch (final Exception e) {
            // 配了信任库却加载不了，必须让连接器启动失败：否则会在第一条记录上以 SSL 异常出现，
            // 被当成"下游不可用"反复重试，把配置错误伪装成故障
            throw new ConnectException("加载信任库失败: " + truststore, e);
        }
    }

    private static SSLContext trustAll() {
        try {
            final SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, new TrustManager[]{new TrustAll()}, new SecureRandom());
            return context;
        } catch (final Exception e) {
            throw new ConnectException("初始化跳过证书校验的 TLS 上下文失败", e);
        }
    }

    /** 只用于联调；生产开启等于放弃传输层身份认证 */
    private static final class TrustAll extends X509ExtendedTrustManager {
        @Override
        public void checkClientTrusted(final X509Certificate[] chain, final String authType) {
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] chain, final String authType) {
        }

        @Override
        public void checkClientTrusted(final X509Certificate[] chain, final String authType,
                                       final java.net.Socket socket) {
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] chain, final String authType,
                                       final java.net.Socket socket) {
        }

        @Override
        public void checkClientTrusted(final X509Certificate[] chain, final String authType,
                                       final SSLEngine engine) {
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] chain, final String authType,
                                       final SSLEngine engine) {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
