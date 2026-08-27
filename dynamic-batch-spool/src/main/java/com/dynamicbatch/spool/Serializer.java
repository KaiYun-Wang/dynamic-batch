package com.dynamicbatch.spool;

/**
 * 序列化器：Spool 内部只存 byte[]，类型转换由本接口完成。
 * <p>使用方实现，不绑定具体序列化框架。建议用 Jackson / FastJson / Kryo。
 */
@FunctionalInterface
public interface Serializer<T> {

    byte[] serialize(T data);

    default T deserialize(byte[] bytes, Class<T> type) {
        throw new UnsupportedOperationException("override if needed");
    }
}