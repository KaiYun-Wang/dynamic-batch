package com.dynamicbatch.common.parser.json;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JacksonJsonParserTest {

    private final JacksonJsonParser parser = new JacksonJsonParser();

    @Test
    public void testSupportsWithJacksonOnClasspath() {
        assertTrue(parser.supports());
    }

    @Test
    public void testToJsonAndFromJsonClass() {
        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");
        String json = parser.toJson(map);
        assertTrue(json.contains("\"key\""));
        assertTrue(json.contains("\"value\""));

        Map<String, Object> back = parser.fromJson(json, Map.class);
        assertEquals("value", back.get("key"));
    }

    @Test
    public void testFromJsonWithType() {
        List<Integer> list = parser.fromJson("[1,2,3]", List.class);
        assertEquals(Arrays.asList(1, 2, 3), list);
    }
}
