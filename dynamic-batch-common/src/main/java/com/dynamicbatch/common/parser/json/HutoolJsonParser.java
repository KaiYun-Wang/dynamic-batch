package com.dynamicbatch.common.parser.json;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;

import java.lang.reflect.Type;

/**
 * 基于 hutool-json 的 JsonParser 适配器。
 *
 * <p>当 classpath 中存在 hutool-all 时通过 META-INF/services 自动加载。
 * 适配器结构参考 dromara dynamic-tp 的 JacksonParser / GsonParser。
 */
public class HutoolJsonParser implements JsonParser {

    @Override
    public String toJson(Object obj) {
        return JSONUtil.toJsonStr(obj);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T fromJson(String json, Class<T> clazz) {
        return (T) JSONUtil.toBean(json, clazz);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T fromJson(String json, Type type) {
        return (T) new JSONObject(json).toBean(type);
    }
}