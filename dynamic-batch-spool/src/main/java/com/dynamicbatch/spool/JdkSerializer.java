package com.dynamicbatch.spool;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * 默认序列化器：JDK 原生序列化（{@link ObjectOutputStream}），零依赖。
 * <p>要求：存储对象必须实现 {@link Serializable}，否则序列化时抛异常。
 * 需要更高性能或可读格式时，用自定义序列化器替代（见 {@link Spool.Builder#serializer(Serializer)}）。</p>
 */
public class JdkSerializer<T> implements Serializer<T> {

    @Override
    public byte[] serialize(T data) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(data);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("serialize failed, data must implement Serializable", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(byte[] bytes, Class<T> type) {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return (T) ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException("deserialize failed", e);
        }
    }
}