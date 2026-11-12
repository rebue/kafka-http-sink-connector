package rebue.kafka.httpsink.json;

import java.util.Locale;

/**
 * 请求体的渲染方式。
 *
 * <p>三种模式解决的是同一个歧义：Kafka 里的"值"既可能是**已经是 JSON 文本的字符串**
 * （上游用 {@code JsonConverter} 且 {@code schemas.enable=false}），也可能是
 * {@link org.apache.kafka.connect.data.Struct}、Map、byte[]。不显式区分就只能靠猜，
 * 猜错的后果是下游收到一段无法解析的脏文本。
 */
public enum PayloadFormat {
    /**
     * 按值的实际类型分派：字符串原样发出（认为上游已产出 JSON 文本），结构化值走 schema
     * 感知的 JSON 序列化。适合绝大多数由 {@code JsonConverter} 喂数据的场景。
     */
    AUTO("auto"),

    /** 一律序列化成 JSON：字符串会被加上引号并转义，结构化值按 schema 展开。 */
    JSON("json"),

    /** 一律当纯文本：只接受字符串与基本类型，不加引号也不转义。 */
    TEXT("text");

    /** 配置里的合法取值，供 {@code ConfigDef.ValidString} 使用 */
    public static final String[] NAMES = {"auto", "json", "text"};

    private final String configValue;

    PayloadFormat(final String configValue) {
        this.configValue = configValue;
    }

    public String configValue() {
        return configValue;
    }

    /** 已由 {@code ValidString} 保证取值合法，这里不再对未知值报错 */
    public static PayloadFormat of(final String raw) {
        if (raw == null) {
            return AUTO;
        }
        return switch (raw.trim().toLowerCase(Locale.ROOT)) {
            case "json" -> JSON;
            case "text" -> TEXT;
            default -> AUTO;
        };
    }
}
