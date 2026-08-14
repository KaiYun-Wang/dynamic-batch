package com.dynamicbatch.common.parser.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;

import java.io.IOException;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 基于 Jackson (jackson-databind + jackson-datatype-jsr310) 的 JsonParser 适配器。
 *
 * <p>classpath 中存在上述两个库时通过 META-INF/services 自动加载。
 * ObjectMapper 按需懒加载（双检锁），配置参考 dromara dynamic-tp 的
 * JacksonParser / JacksonCreator。
 */
public class JacksonJsonParser extends AbstractJsonParser {

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static final String DATABIND_CLASS_NAME = "com.fasterxml.jackson.databind.ObjectMapper";

    private static final String JSR310_CLASS_NAME = "com.fasterxml.jackson.datatype.jsr310.JavaTimeModule";

    /**
     * 必须是实例字段而非 static：若为 static 字段会在类初始化时加载 ObjectMapper，
     * classpath 缺 jackson 时 new JacksonJsonParser() 就直接 NoClassDefFoundError，
     * supports() 探测机制将失效（探测依赖的就是"类能安全 new 出来"）。
     */
    private volatile ObjectMapper mapper;

    @Override
    public String toJson(Object obj) {
        try {
            return getMapper().writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Jackson toJson failed: " + e.getMessage(), e);
        }
    }

    @Override
    public <T> T fromJson(String json, Class<T> clazz) {
        try {
            return getMapper().readValue(json, clazz);
        } catch (IOException e) {
            throw new IllegalStateException("Jackson fromJson failed: " + e.getMessage(), e);
        }
    }

    @Override
    public <T> T fromJson(String json, Type type) {
        try {
            return getMapper().readValue(json, getMapper().constructType(type));
        } catch (IOException e) {
            throw new IllegalStateException("Jackson fromJson failed: " + e.getMessage(), e);
        }
    }

    private ObjectMapper getMapper() {
        // double check lock
        if (mapper == null) {
            synchronized (this) {
                if (mapper == null) {
                    mapper = createMapper();
                }
            }
        }
        return mapper;
    }

    private ObjectMapper createMapper() {
        JavaTimeModule javaTimeModule = new JavaTimeModule();
        javaTimeModule.addSerializer(LocalDateTime.class,
                new LocalDateTimeSerializer(DateTimeFormatter.ofPattern(DATE_FORMAT)));
        javaTimeModule.addDeserializer(LocalDateTime.class,
                new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern(DATE_FORMAT)));
        return JsonMapper.builder()
                .configure(MapperFeature.PROPAGATE_TRANSIENT_MARKER, true)
                // 反序列化时遇到未知属性不报错
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                // 序列化空对象不抛异常
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                // 忽略 null / 空集合等属性
                .serializationInclusion(JsonInclude.Include.NON_EMPTY)
                .addModules(javaTimeModule)
                // 统一日期格式
                .defaultDateFormat(new SimpleDateFormat(DATE_FORMAT))
                .build();
    }

    @Override
    protected String[] getMapperClassNames() {
        return new String[]{DATABIND_CLASS_NAME, JSR310_CLASS_NAME};
    }
}
