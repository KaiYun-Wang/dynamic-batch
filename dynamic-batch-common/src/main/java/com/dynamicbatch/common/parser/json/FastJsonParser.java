package com.dynamicbatch.common.parser.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.lang.reflect.Type;

/**
 * 基于 fastjson 1.x (com.alibaba.fastjson) 的 JsonParser 适配器。
 *
 * <p>classpath 中存在 fastjson 时通过 META-INF/services 自动加载。
 * 适配器结构参考 dromara dynamic-tp 的 FastJsonParser。
 */
public class FastJsonParser extends AbstractJsonParser {

    private static final String FASTJSON_CLASS_NAME = "com.alibaba.fastjson.JSON";

    @Override
    public String toJson(Object obj) {
        return JSON.toJSONString(obj,
                SerializerFeature.WriteDateUseDateFormat,
                SerializerFeature.DisableCircularReferenceDetect);
    }

    @Override
    public <T> T fromJson(String json, Class<T> clazz) {
        return JSON.parseObject(json, clazz);
    }

    @Override
    public <T> T fromJson(String json, Type type) {
        return JSON.parseObject(json, type);
    }

    @Override
    protected String[] getMapperClassNames() {
        return new String[]{FASTJSON_CLASS_NAME};
    }
}
