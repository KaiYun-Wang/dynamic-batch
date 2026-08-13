package com.dynamicbatch.common.parser.json;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * 零依赖的内置 JSON 兜底解析器。
 *
 * <p>仅支持 Map / List / String / Number / Boolean 的序列化与简单反序列化，
 * 当 classpath 中无 hutool-json 等 JSON 库时作为最后的兜底方案。
 * 开发阶段建议直接使用 {@link HutoolJsonParser}。
 */
public class SimpleJsonParser implements JsonParser {

    @Override
    public String toJson(Object obj) {
        if (obj == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder();
        appendValue(sb, obj);
        return sb.toString();
    }

    @Override
    public <T> T fromJson(String json, Class<T> clazz) {
        throw new UnsupportedOperationException("SimpleJsonParser does not support deserialization");
    }

    @Override
    public <T> T fromJson(String json, Type type) {
        throw new UnsupportedOperationException("SimpleJsonParser does not support deserialization");
    }

    @SuppressWarnings("unchecked")
    private static void appendValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof String) {
            appendString(sb, (String) value);
        } else if (value instanceof Number || value instanceof Boolean) {
            sb.append(value);
        } else if (value instanceof Map) {
            appendMap(sb, (Map<String, Object>) value);
        } else if (value instanceof List) {
            appendList(sb, (List<Object>) value);
        } else if (value instanceof Object[]) {
            appendArray(sb, (Object[]) value);
        } else {
            appendString(sb, value.toString());
        }
    }

    private static void appendMap(StringBuilder sb, Map<String, Object> map) {
        sb.append('{');
        boolean first = true;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (first) {
                first = false;
            } else {
                sb.append(',');
            }
            appendString(sb, entry.getKey());
            sb.append(':');
            appendValue(sb, entry.getValue());
        }
        sb.append('}');
    }

    private static void appendList(StringBuilder sb, List<Object> list) {
        sb.append('[');
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            appendValue(sb, list.get(i));
        }
        sb.append(']');
    }

    private static void appendArray(StringBuilder sb, Object[] array) {
        sb.append('[');
        for (int i = 0; i < array.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            appendValue(sb, array[i]);
        }
        sb.append(']');
    }

    private static void appendString(StringBuilder sb, String s) {
        sb.append('"');
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append('"');
    }
}