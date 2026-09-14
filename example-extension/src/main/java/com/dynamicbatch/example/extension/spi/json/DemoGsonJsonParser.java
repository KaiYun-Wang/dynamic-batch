package com.dynamicbatch.example.extension.spi.json;

import com.dynamicbatch.common.parser.json.AbstractJsonParser;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;

/**
 * 自定义 JsonParser SPI 示例：用 Gson 替代 common 内置的 Jackson/Fastjson/Hutool。
 *
 * <p>{@link com.dynamicbatch.common.util.JsonUtil} 通过 ServiceLoader 取<strong>第一个
 * {@code supports()==true}</strong> 的实现。本模块的 {@code META-INF/services} 写在
 * 应用自身 classpath 靠前位置，启动后日志应看到 {@code DemoGsonJsonParser}。
 */
public class DemoGsonJsonParser extends AbstractJsonParser {

    private static final Logger log = LoggerFactory.getLogger(DemoGsonJsonParser.class);

    private static final String GSON_CLASS = "com.google.gson.Gson";

    private final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

    public DemoGsonJsonParser() {
        log.info("DemoGsonJsonParser constructed (SPI candidate)");
    }

    @Override
    public String toJson(Object obj) {
        return gson.toJson(obj);
    }

    @Override
    public <T> T fromJson(String json, Class<T> clazz) {
        try {
            return gson.fromJson(json, clazz);
        } catch (JsonSyntaxException e) {
            throw new IllegalStateException("Gson fromJson failed: " + e.getMessage(), e);
        }
    }

    @Override
    public <T> T fromJson(String json, Type type) {
        try {
            return gson.fromJson(json, type);
        } catch (JsonSyntaxException e) {
            throw new IllegalStateException("Gson fromJson failed: " + e.getMessage(), e);
        }
    }

    @Override
    protected String[] getMapperClassNames() {
        return new String[]{GSON_CLASS};
    }
}
