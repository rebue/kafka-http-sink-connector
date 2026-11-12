package rebue.kafka.httpsink;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Date;
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
 * 把 Connect 记录的值渲染成 HTTP 请求体。
 *
 * <p>为什么不直接用 {@code value.toString()}（老实现的做法）：{@code value.converter} 打开
 * {@code schemas.enable} 时，SinkTask 拿到的是 {@link Struct}，它的 {@code toString()} 是
 * {@code Struct{id=1, ...}}，不是 JSON；下游只会收到一段无法解析的脏文本。
 *
 * <p>为什么不打包 {@code connect-json} + Jackson：插件包越小、越自包含，越不容易和 worker
 * 自带的 Jackson 版本打架。这里的实现只依赖 {@code connect-api}，覆盖 Connect 的全部
 * {@link Type} 与常用逻辑类型（Decimal / Date / Time / Timestamp）。
 */
public final class Json {
    private Json() {
    }

    /**
     * 把一条记录的值渲染成请求体。
     *
     * @param value         记录值，非 null（tombstone 由调用方跳过）
     * @param format        {@code auto} / {@code string} / {@code json} / {@code bytes}
     * @param bytesEncoding {@code base64} / {@code utf8}，只影响 byte[]
     * @return 请求体文本，永不为 null
     */
    public static String toBody(final Object value, final String format, final String bytesEncoding) {
        if (value == null) {
            return "null";
        }
        final boolean base64 = !"utf8".equalsIgnoreCase(bytesEncoding);
        switch (format) {
            case "string":
                return String.valueOf(value);
            case "bytes":
                return encodeBytes(requireBytes(value), base64);
            case "json":
                return render(value, base64);
            case "auto":
            default:
                // 上游多用 JsonConverter + schemas.enable=false，此时 value 已经是 JSON 文本，
                // 原样发出即可；再序列化一次会变成带引号的字符串
                if (value instanceof CharSequence || value instanceof Character) {
                    return value.toString();
                }
                if (value instanceof byte[] || value instanceof ByteBuffer) {
                    return encodeBytes(requireBytes(value), base64);
                }
                if (value instanceof Number || value instanceof Boolean) {
                    return value.toString();
                }
                return render(value, base64);
        }
    }

    /**
     * 把多个已渲染好的请求体拼成一个 JSON 数组（批量投递）。
     *
     * <p>拼接而不是重新序列化：单条渲染已经把每个元素转成了 JSON 文本，重新解析再序列化
     * 只会多一次失败机会。
     */
    public static String toBatchBody(final Iterable<String> bodies) {
        final StringBuilder sb = new StringBuilder(64);
        sb.append('[');
        boolean first = true;
        for (final String body : bodies) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append(body);
        }
        sb.append(']');
        return sb.toString();
    }

    private static String render(final Object value, final boolean base64) {
        final StringBuilder sb = new StringBuilder(64);
        writeValue(value, sb, base64);
        return sb.toString();
    }

    private static void writeValue(final Object value, final StringBuilder sb, final boolean base64) {
        if (value == null) {
            sb.append("null");
            return;
        }
        if (value instanceof CharSequence || value instanceof Character) {
            quote(value.toString(), sb);
            return;
        }
        if (value instanceof Boolean) {
            sb.append(value);
            return;
        }
        if (value instanceof BigDecimal) {
            sb.append(((BigDecimal) value).toPlainString());
            return;
        }
        if (value instanceof Double) {
            final double number = (Double) value;
            if (Double.isNaN(number) || Double.isInfinite(number)) {
                quote(String.valueOf(number), sb);
            } else {
                sb.append(value);
            }
            return;
        }
        if (value instanceof Float) {
            final float number = (Float) value;
            if (Float.isNaN(number) || Float.isInfinite(number)) {
                quote(String.valueOf(number), sb);
            } else {
                sb.append(value);
            }
            return;
        }
        if (value instanceof Number) {
            sb.append(value);
            return;
        }
        if (value instanceof byte[] || value instanceof ByteBuffer) {
            quote(encodeBytes(requireBytes(value), base64), sb);
            return;
        }
        if (value instanceof Struct) {
            writeStruct((Struct) value, sb, base64);
            return;
        }
        if (value instanceof Map) {
            writeMap((Map<?, ?>) value, sb, base64);
            return;
        }
        if (value instanceof Iterable) {
            writeIterable((Iterable<?>) value, sb, base64);
            return;
        }
        if (value.getClass().isArray()) {
            writeArray(value, sb, base64);
            return;
        }
        if (value instanceof Date) {
            quote(toInstant((Date) value), sb);
            return;
        }
        // 兜底必须报错：静默 toString 会把 Struct{...} 这类非 JSON 文本发给下游，
        // 比直接失败更难排查
        throw new DataException("无法把 " + value.getClass().getName()
                + " 序列化为请求体，请把 value.format 设为 string 或调整 value.converter");
    }

    private static void writeStruct(final Struct struct, final StringBuilder sb, final boolean base64) {
        final Schema schema = struct.schema();
        sb.append('{');
        boolean first = true;
        for (final Field field : schema.fields()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            quote(field.name(), sb);
            sb.append(':');
            writeLogical(field.schema(), struct.get(field), sb, base64);
        }
        sb.append('}');
    }

    /**
     * 按 schema 的**逻辑类型**渲染值。
     *
     * <p>不按逻辑类型处理的话，Decimal 会被当成普通 bytes 输出成 BASE64，下游拿到的金额是
     * 一串乱码；Timestamp 会变成 epoch 数字，丢失可读性。
     */
    private static void writeLogical(final Schema schema, final Object value,
                                     final StringBuilder sb, final boolean base64) {
        if (value == null) {
            sb.append("null");
            return;
        }
        final String logicalName = schema == null ? null : schema.name();
        if (Decimal.LOGICAL_NAME.equals(logicalName) && value instanceof BigDecimal) {
            sb.append(((BigDecimal) value).toPlainString());
            return;
        }
        if (Timestamp.LOGICAL_NAME.equals(logicalName) && value instanceof Date) {
            quote(toInstant((Date) value), sb);
            return;
        }
        if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(logicalName) && value instanceof Date) {
            quote(Instant.ofEpochMilli(((Date) value).getTime())
                    .atZone(ZoneOffset.UTC).toLocalDate().toString(), sb);
            return;
        }
        if (Time.LOGICAL_NAME.equals(logicalName) && value instanceof Date) {
            quote(Instant.ofEpochMilli(((Date) value).getTime())
                    .atZone(ZoneOffset.UTC).toLocalTime().toString(), sb);
            return;
        }
        if (schema != null && schema.type() == Type.BYTES) {
            quote(encodeBytes(requireBytes(value), base64), sb);
            return;
        }
        writeValue(value, sb, base64);
    }

    private static void writeMap(final Map<?, ?> map, final StringBuilder sb, final boolean base64) {
        sb.append('{');
        boolean first = true;
        for (final Map.Entry<?, ?> entry : map.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            quote(String.valueOf(entry.getKey()), sb);
            sb.append(':');
            writeValue(entry.getValue(), sb, base64);
        }
        sb.append('}');
    }

    private static void writeIterable(final Iterable<?> values, final StringBuilder sb, final boolean base64) {
        sb.append('[');
        boolean first = true;
        for (final Object value : values) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            writeValue(value, sb, base64);
        }
        sb.append(']');
    }

    private static void writeArray(final Object array, final StringBuilder sb, final boolean base64) {
        sb.append('[');
        final int length = Array.getLength(array);
        for (int i = 0; i < length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            writeValue(Array.get(array, i), sb, base64);
        }
        sb.append(']');
    }

    /** JSON 字符串转义：控制字符必须转义，否则下游解析直接失败 */
    private static void quote(final String text, final StringBuilder sb) {
        sb.append('"');
        for (int i = 0; i < text.length(); i++) {
            final char ch = text.charAt(i);
            switch (ch) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (ch < 0x20) {
                        sb.append(String.format("\\u%04x", (int) ch));
                    } else {
                        sb.append(ch);
                    }
                }
            }
        }
        sb.append('"');
    }

    private static String toInstant(final Date value) {
        return Instant.ofEpochMilli(value.getTime()).toString();
    }

    private static String encodeBytes(final byte[] bytes, final boolean base64) {
        return base64
                ? Base64.getEncoder().encodeToString(bytes)
                : new String(bytes, StandardCharsets.UTF_8);
    }

    private static byte[] requireBytes(final Object value) {
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof ByteBuffer) {
            final ByteBuffer buffer = ((ByteBuffer) value).duplicate();
            final byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        }
        throw new DataException("value.format=bytes 只适用于 byte[] / ByteBuffer，实际是 "
                + value.getClass().getName());
    }
}
