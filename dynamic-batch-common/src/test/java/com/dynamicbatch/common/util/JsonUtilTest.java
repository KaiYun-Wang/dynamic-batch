package com.dynamicbatch.common.util;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class JsonUtilTest {

    @Test
    public void testToJsonBasicMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");
        String json = JsonUtil.toJson(map);
        assertTrue(json.contains("\"key\""));
        assertTrue(json.contains("\"value\""));
    }

    @Test
    public void testToJsonNestedMap() {
        Map<String, Object> inner = new HashMap<>();
        inner.put("a", 1);
        Map<String, Object> outer = new HashMap<>();
        outer.put("data", inner);

        String json = JsonUtil.toJson(outer);
        assertTrue(json.contains("\"a\":1"));
        assertTrue(json.contains("\"data\""));
    }

    @Test
    public void testToJsonStringArray() {
        Map<String, Object> map = new HashMap<>();
        map.put("tags", new String[]{"x", "y"});

        String json = JsonUtil.toJson(map);
        assertTrue(json.contains("\"x\""));
        assertTrue(json.contains("\"y\""));
    }
}