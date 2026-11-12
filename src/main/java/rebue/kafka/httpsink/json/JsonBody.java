package rebue.kafka.httpsink.json;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

/**
 * Connect 记录值 → HTTP 请求体。
 *
 * <p>为什么不用 {@code value.toString()}：当 worker 的 {@code value.converter} 打开
 * {@code schemas.enable} 时，SinkTask 拿到的是 {@link Struct}，它的 {@code toString()} 是
 * {@code Struct{id=1, ...}} —— 不是 JSON，下游只能收到一段无法解析的脏文本。
 *
 * <p>为什么不打包 {@code connect-json} + Jackson：插件应当只依赖 {@code connect-api}，
 * 包里多一份 Jackson 就有和 worker 自带版本打架的风险（classloader 先命中谁取决于
 * 插件路径顺序）。这里覆盖 Connect 的全部 {@link Type} 与会被 {@code JsonConverter}
 * 用到的逻辑类型（Decimal / Date / Time / Timestamp）。
 *
 * <p>实例是有状态的（复用 {@link StringBuilder}），但一个实例只在单线程内使用。
 */
public final class JsonBody {
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    /** 递归深度上限：schema 环或用户的超深嵌套不该把栈打穿 */
    private static final int MAX_DEPTH = 64;

    private final StringBuilder buffer = new StringBuilder(256);
    private final boolean bytesAsBase64;

    public JsonBody(final boolean bytesAsBase64) {
        this.bytesAsBase64 = bytesAsBase64;
    }

    /**
     * 渲染一条记录的值。
     *
     * @param value  非 null（tombstone 由调用方跳过）
     * @param format {@link PayloadFormat}
     * @return 请求体文本，永不为 null
     * @throws DataException 值无法按所选格式渲染（属于记录级问题，会进 DLQ）
     */
    public String render(final Object value, final PayloadFormat format) {
        if (value == null) {
            return "null";
        }
        buffer.setLength(0);
        switch (format) {
            case TEXT -> appendText(value);
            case JSON -> write(value, 0);
            case AUTO -> {
                if (isRawScalar(value)) {
                    appendText(value);
                } else {
                    write(value, 0);
                }
            }
        }
        return buffer.toString();
    }

    /** 把若干已渲染好的元素拼成 JSON 数组；拼接而不是重解析，少一次失败机会 */
    public String array(final List<String> elements) {
        buffer.setLength(0);
        buffer.append('[');
        for (int i = 0; i < elements.size(); i++) {
            if (i > 0) {
                buffer.append(',');
            }
            buffer.append(elements.get(i));
        }
        buffer.append(']');
        return buffer.toString();
    }

    /* ------------------------------------------------------------------ 文本模式 */

    private void appendText(final Object value) {
        if (value instanceof CharSequence || value instanceof Character) {
            buffer.append(value);
            return;
        }
        if (value instanceof Number || value instanceof Boolean) {
            buffer.append(value);
            return;
        }
        if (value instanceof byte[] bytes) {
            buffer.append(encodeBytes(bytes));
            return;
        }
        if (value instanceof ByteBuffer bufferView) {
            buffer.append(encodeBytes(toBytes(bufferView)));
            return;
        }
        throw new DataException("http.payload.format=text 只接受字符串、基本类型或字节数组，实际是 "
                + value.getClass().getName() + "；结构化数据请用 auto 或 json");
    }

    /* ------------------------------------------------------------------ JSON 模式 */

    private void write(final Object value, final int depth) {
        if (depth > MAX_DEPTH) {
            throw new DataException("值嵌套超过 " + MAX_DEPTH + " 层，拒绝序列化");
        }
        if (value == null) {
            buffer.append("null");
            return;
        }
        if (value instanceof CharSequence || value instanceof Character) {
            quote(value.toString(), buffer);
            return;
        }
        if (value instanceof Boolean) {
            buffer.append(value);
            return;
        }
        if (value instanceof BigDecimal decimal) {
            // toPlainString：否则 1E+3 这种科学计数法会让下游解析成字符串或直接失败
            buffer.append(decimal.toPlainString());
            return;
        }
        if (value instanceof Double number) {
            buffer.append(jsonNumber(number, number.doubleValue()));
            return;
        }
        if (value instanceof Float number) {
            buffer.append(jsonNumber(number, number.doubleValue()));
            return;
        }
        if (value instanceof Number) {
            buffer.append(value);
            return;
        }
        if (value instanceof byte[] bytes) {
            quote(encodeBytes(bytes), buffer);
            return;
        }
        if (value instanceof ByteBuffer bufferView) {
            quote(encodeBytes(toBytes(bufferView)), buffer);
            return;
        }
        if (value instanceof Struct struct) {
            writeStruct(struct, depth);
            return;
        }
        if (value instanceof Map<?, ?> map) {
            writeMap(map, depth);
            return;
        }
        if (value instanceof Iterable<?> iterable) {
            writeIterable(iterable, depth);
            return;
        }
        if (value.getClass().isArray()) {
            writeArray(value, depth);
            return;
        }
        if (value instanceof Date date) {
            quote(Instant.ofEpochMilli(date.getTime()).toString(), buffer);
            return;
        }
        // 兜底必须报错：静默 toString 只会把 Struct{...} 这类非 JSON 文本发出去，
        // 排查成本比直接失败高得多
        throw new DataException("无法把 " + value.getClass().getName()
                + " 序列化为 JSON，请改用 http.payload.format=text 或调整上游 value.converter");
    }

    /** JSON 没有 NaN / Infinity：退化成字符串，保住信息且不破坏报文合法性 */
    private static String jsonNumber(final Number original, final double primitive) {
        return Double.isNaN(primitive) || Double.isInfinite(primitive)
                ? "\"" + original + "\""
                : original.toString();
    }

    private void writeStruct(final Struct struct, final int depth) {
        buffer.append('{');
        boolean first = true;
        for (final Field field : struct.schema().fields()) {
            if (!first) {
                buffer.append(',');
            }
            first = false;
            quote(field.name(), buffer);
            buffer.append(':');
            writeField(field, struct.get(field), depth + 1);
        }
        buffer.append('}');
    }

    /**
     * 按 schema 的**逻辑类型**渲染一个字段。
     *
     * <p>不看逻辑类型的话，{@code Decimal} 会按物理类型 BYTES 输出成 BASE64，下游拿到的
     * 金额是一串乱码；{@code Timestamp} 会变成 epoch 数字，可读性尽失。
     */
    private void writeField(final Field field, final Object value, final int depth) {
        if (value == null) {
            buffer.append("null");
            return;
        }
        final Schema schema = field.schema();
        final String logical = schema.name();
        if (Decimal.LOGICAL_NAME.equals(logical) && value instanceof BigDecimal decimal) {
            buffer.append(decimal.toPlainString());
            return;
        }
        if (Timestamp.LOGICAL_NAME.equals(logical) && value instanceof Date date) {
            quote(Instant.ofEpochMilli(date.getTime()).toString(), buffer);
            return;
        }
        if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(logical) && value instanceof Date date) {
            quote(Instant.ofEpochMilli(date.getTime()).atZone(ZoneOffset.UTC).toLocalDate().toString(), buffer);
            return;
        }
        if (Time.LOGICAL_NAME.equals(logical) && value instanceof Date date) {
            quote(Instant.ofEpochMilli(date.getTime()).atZone(ZoneOffset.UTC).toLocalTime().toString(), buffer);
            return;
        }
        if (schema.type() == Type.BYTES && !(value instanceof byte[]) && !(value instanceof ByteBuffer)) {
            throw new DataException("字段 " + field.name() + " 声明为 BYTES，实际是 "
                    + value.getClass().getName());
        }
        write(value, depth);
    }

    private void writeMap(final Map<?, ?> map, final int depth) {
        buffer.append('{');
        boolean first = true;
        for (final Map.Entry<?, ?> entry : map.entrySet()) {
            if (!first) {
                buffer.append(',');
            }
            first = false;
            quote(String.valueOf(entry.getKey()), buffer);
            buffer.append(':');
            write(entry.getValue(), depth + 1);
        }
        buffer.append('}');
    }

    private void writeIterable(final Iterable<?> values, final int depth) {
        buffer.append('[');
        boolean first = true;
        for (final Object value : values) {
            if (!first) {
                buffer.append(',');
            }
            first = false;
            write(value, depth + 1);
        }
        buffer.append(']');
    }

    private void writeArray(final Object array, final int depth) {
        buffer.append('[');
        final int length = Array.getLength(array);
        for (int i = 0; i < length; i++) {
            if (i > 0) {
                buffer.append(',');
            }
            write(Array.get(array, i), depth + 1);
        }
        buffer.append(']');
    }

    /* ------------------------------------------------------------------ 工具 */

    /**
     * 写一个 JSON 字符串字面量。
     *
     * <p>逐段追加而不是逐字符 append：绝大多数字符不需要转义，按"命中才切段"处理，
     * 少一次字符拷贝。控制字符必须转义，否则下游 JSON 解析直接失败。
     */
    public static void quote(final String text, final StringBuilder out) {
        out.append('"');
        int segment = 0;
        for (int i = 0; i < text.length(); i++) {
            final String escape = escapeOf(text.charAt(i));
            if (escape == null) {
                continue;
            }
            if (segment < i) {
                out.append(text, segment, i);
            }
            out.append(escape);
            segment = i + 1;
        }
        if (segment < text.length()) {
            out.append(text, segment, text.length());
        }
        out.append('"');
    }

    private static String escapeOf(final char ch) {
        return switch (ch) {
            case '"' -> "\\\"";
            case '\\' -> "\\\\";
            case '\b' -> "\\b";
            case '\f' -> "\\f";
            case '\n' -> "\\n";
            case '\r' -> "\\r";
            case '\t' -> "\\t";
            default -> ch < 0x20 ? unicode(ch) : null;
        };
    }

    private static String unicode(final char ch) {
        return new String(new char[]{'\\', 'u',
                HEX[(ch >> 12) & 0xF], HEX[(ch >> 8) & 0xF], HEX[(ch >> 4) & 0xF], HEX[ch & 0xF]});
    }

    private static boolean isRawScalar(final Object value) {
        return value instanceof CharSequence || value instanceof Character
                || value instanceof Number || value instanceof Boolean
                || value instanceof byte[] || value instanceof ByteBuffer;
    }

    private String encodeBytes(final byte[] bytes) {
        return bytesAsBase64 ? Base64.getEncoder().encodeToString(bytes) : new String(bytes, StandardCharsets.UTF_8);
    }

    private static byte[] toBytes(final ByteBuffer buffer) {
        final ByteBuffer view = buffer.duplicate();
        final byte[] bytes = new byte[view.remaining()];
        view.get(bytes);
        return bytes;
    }
}
