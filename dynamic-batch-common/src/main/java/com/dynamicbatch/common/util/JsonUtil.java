package com.dynamicbatch.common.util;

import com.dynamicbatch.common.parser.json.JsonParser;
import com.dynamicbatch.common.parser.json.SimpleJsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.List;

/**
 * JSON 工具类。
 *
 * <p>通过 {@link ExtensionServiceLoader} 加载 {@link JsonParser} 实现；
 * SPI 未找到任何有效实现时，回退到 {@link SimpleJsonParser}（零依赖兜底）。
 * 使用方式同 dynamic-tp 的 JsonUtil。
 */
public class JsonUtil {

    private static final Logger log = LoggerFactory.getLogger(JsonUtil.class);

    private static final JsonParser JSON_PARSER;

    static {
        JsonParser parser = null;
        List<JsonParser> parsers = ExtensionServiceLoader.get(JsonParser.class);
        if (parsers != null && !parsers.isEmpty()) {
            parser = parsers.get(0);
            log.info("JsonUtil using parser: {}", parser.getClass().getName());
        }
        if (parser == null) {
            parser = new SimpleJsonParser();
            log.info("JsonUtil using fallback parser: SimpleJsonParser");
        }
        JSON_PARSER = parser;
    }

    private JsonUtil() {
    }

    /**
     * 将对象序列化为 JSON 字符串。
     *
     * @param obj 任意对象
     * @return JSON 字符串
     */
    public static String toJson(Object obj) {
        return JSON_PARSER.toJson(obj);
    }

    /**
     * 将 JSON 字符串反序列化为指定类型的对象。
     *
     * @param json  JSON 字符串
     * @param clazz 目标类型
     * @param <T>   目标类型
     * @return 反序列化结果
     */
    public static <T> T fromJson(String json, Class<T> clazz) {
        return JSON_PARSER.fromJson(json, clazz);
    }

    /**
     * 将 JSON 字符串反序列化为指定泛型类型的对象。
     *
     * @param json JSON 字符串
     * @param type 目标类型（可用于泛型）
     * @param <T>  目标类型
     * @return 反序列化结果
     */
    public static <T> T fromJson(String json, Type type) {
        return JSON_PARSER.fromJson(json, type);
    }
}