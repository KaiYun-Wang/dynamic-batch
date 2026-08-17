package com.dynamicbatch.common.parser.json;

import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * SimpleJsonParser 序列化/反序列化能力测试：与 JsonUtil 对外 API 对齐，
 * 保证无三方库环境下（兜底解析器）行为一致。
 */
public class SimpleJsonParserTest {

    private final SimpleJsonParser parser = new SimpleJsonParser();

    @Test
    public void roundTripMapWithAllValueTypes() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("name", "device");
        map.put("count", 100);
        map.put("ratio", 1.5);
        map.put("enabled", true);
        map.put("tags", new String[]{"a", "b"});
        map.put("nested", null);

        String json = parser.toJson(map);
        Map<?, ?> parsed = parser.fromJson(json, Map.class);

        assertEquals("device", parsed.get("name"));
        assertEquals(100L, ((Number) parsed.get("count")).longValue());
        assertEquals(1.5d, ((Number) parsed.get("ratio")).doubleValue(), 0.0001);
        assertEquals(Boolean.TRUE, parsed.get("enabled"));
        assertEquals(2, ((List<?>) parsed.get("tags")).size());
        assertNull(parsed.get("nested"));
    }

    @Test
    public void stringEscapesRoundTrip() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("text", "hello \"quoted\" \n newline \\ backslash \t tab");

        String json = parser.toJson(map);
        Map<?, ?> parsed = parser.fromJson(json, Map.class);

        assertEquals("hello \"quoted\" \n newline \\ backslash \t tab", parsed.get("text"));
    }

    @Test
    public void parseUnicodeEscape() {
        Map<?, ?> parsed = parser.fromJson("{\"s\":\"\\u4f60\\u597d\"}", Map.class);
        assertEquals("你好", parsed.get("s"));
    }

    @Test
    public void parseExternalJsonWithWhitespace() {
        Map<?, ?> parsed = parser.fromJson("{ \"a\" : [1, 2.5, \"x\", true, null], \"b\": {} }", Map.class);

        List<?> a = (List<?>) parsed.get("a");
        assertEquals(1L, ((Number) a.get(0)).longValue());
        assertEquals(2.5d, ((Number) a.get(1)).doubleValue(), 0.0001);
        assertEquals("x", a.get(2));
        assertEquals(Boolean.TRUE, a.get(3));
        assertNull(a.get(4));
        assertTrue(((Map<?, ?>) parsed.get("b")).isEmpty());
    }

    @Test
    public void parseEmptyAndLiterals() {
        assertEquals(Boolean.TRUE, parser.fromJson("true", Boolean.class));
        assertEquals(Boolean.FALSE, parser.fromJson("false", Boolean.class));
        assertNull(parser.fromJson("null", Object.class));
        assertTrue(((Map<?, ?>) parser.fromJson("{}", Map.class)).isEmpty());
        assertTrue(((List<?>) parser.fromJson("[]", List.class)).isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseInvalidJsonThrows() {
        parser.fromJson("{\"a\": }", Map.class);
    }
}
