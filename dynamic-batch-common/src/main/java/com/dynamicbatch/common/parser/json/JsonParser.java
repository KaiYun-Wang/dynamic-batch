package com.dynamicbatch.common.parser.json;

import java.lang.reflect.Type;

/**
 * JSON 序列化 / 反序列化 SPI 接口。
 *
 * <p>通过 {@link java.util.ServiceLoader} 加载，实现类可在不修改核心代码的前提下
 * 替换 JSON 引擎。默认提供 {@link HutoolJsonParser}（依赖 hutool-json）和
 * {@link SimpleJsonParser}（零依赖内置兜底）。
 * 设计参考 dromara dynamic-tp 的 common/parser/json/JsonParser。
 */
public interface JsonParser {

    /**
     * 将对象序列化为 JSON 字符串。
     *
     * @param obj 任意对象（Map / List / 基本类型 / POJO）
     * @return JSON 字符串
     */
    String toJson(Object obj);

    /**
     * 将 JSON 字符串反序列化为指定类型的对象。
     *
     * @param json  JSON 字符串
     * @param clazz 目标类型
     * @param <T>   目标类型
     * @return 反序列化结果
     */
    <T> T fromJson(String json, Class<T> clazz);

    /**
     * 将 JSON 字符串反序列化为指定泛型类型的对象。
     *
     * @param json  JSON 字符串
     * @param type  目标类型（可用于泛型，如 {@code List<String>}）
     * @param <T>   目标类型
     * @return 反序列化结果
     */
    <T> T fromJson(String json, Type type);
}