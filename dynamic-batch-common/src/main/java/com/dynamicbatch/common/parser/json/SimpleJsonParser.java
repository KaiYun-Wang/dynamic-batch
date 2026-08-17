package com.dynamicbatch.common.parser.json;

import com.dynamicbatch.common.util.JsonUtil;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 零依赖的内置 JSON 兜底解析器。
 *
 * <p>支持 Map / List / String / Number / Boolean / null 的序列化与反序列化，
 * 与 {@link JsonUtil} 对外 API 能力对齐，保证无三方库环境下行为一致；
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
    @SuppressWarnings("unchecked")
    public <T> T fromJson(String json, Class<T> clazz) {
        return (T) parseValue(json, new int[]{0});
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T fromJson(String json, Type type) {
        return (T) parseValue(json, new int[]{0});
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

    // ======================== 反序列化（迷你 JSON 解析器） ========================

    /** 解析一个 JSON 值；pos[0] 为游标，解析后指向下一个未消费字符 */
    private static Object parseValue(String json, int[] pos) {
        skipWhitespace(json, pos);
        if (pos[0] >= json.length()) {
            throw new IllegalArgumentException("unexpected end of json");
        }
        char c = json.charAt(pos[0]);
        switch (c) {
            case '{':
                return parseMap(json, pos);
            case '[':
                return parseList(json, pos);
            case '"':
                return parseString(json, pos);
            case 't':
                expect(json, pos, "true");
                return Boolean.TRUE;
            case 'f':
                expect(json, pos, "false");
                return Boolean.FALSE;
            case 'n':
                expect(json, pos, "null");
                return null;
            default:
                return parseNumber(json, pos);
        }
    }

    private static Map<String, Object> parseMap(String json, int[] pos) {
        Map<String, Object> map = new LinkedHashMap<>();
        pos[0]++; // {
        skipWhitespace(json, pos);
        if (peek(json, pos) == '}') {
            pos[0]++;
            return map;
        }
        while (true) {
            skipWhitespace(json, pos);
            String key = parseString(json, pos);
            skipWhitespace(json, pos);
            expectChar(json, pos, ':');
            map.put(key, parseValue(json, pos));
            skipWhitespace(json, pos);
            char c = json.charAt(pos[0]);
            if (c == ',') {
                pos[0]++;
                continue;
            }
            if (c == '}') {
                pos[0]++;
                return map;
            }
            throw new IllegalArgumentException("unexpected char in object: " + c);
        }
    }

    private static List<Object> parseList(String json, int[] pos) {
        List<Object> list = new ArrayList<>();
        pos[0]++; // [
        skipWhitespace(json, pos);
        if (peek(json, pos) == ']') {
            pos[0]++;
            return list;
        }
        while (true) {
            list.add(parseValue(json, pos));
            skipWhitespace(json, pos);
            char c = json.charAt(pos[0]);
            if (c == ',') {
                pos[0]++;
                continue;
            }
            if (c == ']') {
                pos[0]++;
                return list;
            }
            throw new IllegalArgumentException("unexpected char in array: " + c);
        }
    }

    private static String parseString(String json, int[] pos) {
        pos[0]++; // "
        StringBuilder sb = new StringBuilder();
        while (true) {
            if (pos[0] >= json.length()) {
                throw new IllegalArgumentException("unterminated string");
            }
            char c = json.charAt(pos[0]++);
            if (c == '"') {
                return sb.toString();
            }
            if (c != '\\') {
                sb.append(c);
                continue;
            }
            if (pos[0] >= json.length()) {
                throw new IllegalArgumentException("unterminated escape");
            }
            char e = json.charAt(pos[0]++);
            switch (e) {
                case '"':
                    sb.append('"');
                    break;
                case '\\':
                    sb.append('\\');
                    break;
                case '/':
                    sb.append('/');
                    break;
                case 'b':
                    sb.append('\b');
                    break;
                case 'f':
                    sb.append('\f');
                    break;
                case 'n':
                    sb.append('\n');
                    break;
                case 'r':
                    sb.append('\r');
                    break;
                case 't':
                    sb.append('\t');
                    break;
                case 'u':
                    if (pos[0] + 4 > json.length()) {
                        throw new IllegalArgumentException("invalid unicode escape");
                    }
                    sb.append((char) Integer.parseInt(json.substring(pos[0], pos[0] + 4), 16));
                    pos[0] += 4;
                    break;
                default:
                    throw new IllegalArgumentException("invalid escape: \\" + e);
            }
        }
    }

    private static Object parseNumber(String json, int[] pos) {
        int start = pos[0];
        while (pos[0] < json.length() && "+-.0123456789eE".indexOf(json.charAt(pos[0])) >= 0) {
            pos[0]++;
        }
        String num = json.substring(start, pos[0]);
        try {
            return Long.parseLong(num);
        } catch (NumberFormatException ignored) {
            try {
                return Double.parseDouble(num);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("invalid number: " + num);
            }
        }
    }

    private static void expect(String json, int[] pos, String literal) {
        if (!json.startsWith(literal, pos[0])) {
            throw new IllegalArgumentException("invalid literal, expected: " + literal);
        }
        pos[0] += literal.length();
    }

    private static void expectChar(String json, int[] pos, char expected) {
        if (pos[0] >= json.length() || json.charAt(pos[0]) != expected) {
            throw new IllegalArgumentException("expected char: " + expected);
        }
        pos[0]++;
    }

    private static char peek(String json, int[] pos) {
        return pos[0] < json.length() ? json.charAt(pos[0]) : '\0';
    }

    private static void skipWhitespace(String json, int[] pos) {
        while (pos[0] < json.length() && Character.isWhitespace(json.charAt(pos[0]))) {
            pos[0]++;
        }
    }
}