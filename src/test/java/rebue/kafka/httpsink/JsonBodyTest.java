package rebue.kafka.httpsink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;

import rebue.kafka.httpsink.json.JsonBody;
import rebue.kafka.httpsink.json.PayloadFormat;

class JsonBodyTest {
    private final JsonBody json = new JsonBody(true);

    private static final Schema ORDER = SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("amount", Decimal.schema(2))
            .field("created", Timestamp.SCHEMA)
            .field("note", Schema.OPTIONAL_STRING_SCHEMA)
            .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .build();

    private static Struct order() {
        return new Struct(ORDER)
                .put("id", 7L)
                .put("amount", new BigDecimal("12.34"))
                .put("created", new Date(1_700_000_000_000L))
                .put("tags", List.of("a", "b"));
    }

    @Test
    void rendersStructWithLogicalTypes() {
        final String body = json.render(order(), PayloadFormat.JSON);
        assertEquals("{\"id\":7,\"amount\":12.34,\"created\":\"2023-11-14T22:13:20Z\","
                + "\"note\":null,\"tags\":[\"a\",\"b\"]}", body);
    }

    /** Decimal 若按物理类型（BYTES）输出会变成 BASE64，下游拿到的金额是乱码 */
    @Test
    void decimalUsesPlainString() {
        final Schema schema = SchemaBuilder.struct().field("amount", Decimal.schema(4)).build();
        final Struct value = new Struct(schema).put("amount", new BigDecimal("1E+3"));
        assertEquals("{\"amount\":1000}", json.render(value, PayloadFormat.JSON));
    }

    /** JSON 没有 NaN：降级成字符串，既保住信息又不破坏报文合法性 */
    @Test
    void nonFiniteNumberBecomesString() {
        assertEquals("\"NaN\"", json.render(Double.NaN, PayloadFormat.JSON));
        assertEquals("\"Infinity\"", json.render(Double.POSITIVE_INFINITY, PayloadFormat.JSON));
    }

    @Test
    void escapesControlCharactersAndQuotes() {
        assertEquals("\"a\\\"b\\\\c\\nd\\te\\u0001\"",
                json.render("a\"b\\c\nd\te\u0001", PayloadFormat.JSON));
    }

    @Test
    void detectsSchemaLessMapAndNestedCollections() {
        final Map<String, Object> value = new LinkedHashMap<>();
        value.put("a", 1);
        value.put("b", List.of(Map.of("c", true)));
        assertEquals("{\"a\":1,\"b\":[{\"c\":true}]}", json.render(value, PayloadFormat.JSON));
    }

    @Test
    void encodesBytesAsBase64() {
        final byte[] bytes = "hi".getBytes(StandardCharsets.UTF_8);
        assertEquals("\"aGk=\"", json.render(bytes, PayloadFormat.JSON));
        assertEquals("\"aGk=\"", json.render(ByteBuffer.wrap(bytes), PayloadFormat.JSON));
    }

    /** auto：上游用 JsonConverter 时值是 JSON 文本，原样发出，不能再套一层引号 */
    @Test
    void autoPassesJsonTextThrough() {
        assertEquals("{\"a\":1}", json.render("{\"a\":1}", PayloadFormat.AUTO));
        assertEquals("42", json.render(42, PayloadFormat.AUTO));
    }

    @Test
    void textModeRejectsStructuredValue() {
        final DataException error = assertThrows(DataException.class,
                () -> json.render(order(), PayloadFormat.TEXT));
        assertTrue(error.getMessage().contains("text"));
    }

    @Test
    void joinsBatchElements() {
        assertEquals("[{\"a\":1},2]", json.array(List.of("{\"a\":1}", "2")));
    }

    @Test
    void rejectsUnknownTypesInsteadOfSilentlyToString() {
        final DataException error = assertThrows(DataException.class,
                () -> json.render(new Object(), PayloadFormat.JSON));
        assertTrue(error.getMessage().contains("无法把"));
    }
}
