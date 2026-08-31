package com.dynamicbatch.core.serializer;

import com.dynamicbatch.common.pojo.SpoolEntryPOJO;
import com.dynamicbatch.spool.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Spool 落盘序列化器：把 {@link SpoolEntryPOJO} 拼成一段 byte[]，从左到右三块：
 * <pre>
 * ┌────────────────┬─────────────────────┬──────────────────────────┐
 * │ ① key 字节长度   │ ② routingKey 二进制   │ ③ payload 序列化结果      │
 * │   (4 字节 int)  │   (UTF-8)           │   (用户 Serializer 决定)  │
 * └────────────────┴─────────────────────┴──────────────────────────┘
 * </pre>
 * serialize 按此拼接，deserialize 按此拆开。帧格式改动会破坏已落盘数据。
 */
public class SpoolEntrySerializer<T> implements Serializer<SpoolEntryPOJO<T>> {

    private final Class<T> payloadType;
    private final Serializer<T> payloadSerializer;

    public SpoolEntrySerializer(Class<T> payloadType, Serializer<T> payloadSerializer) {
        this.payloadType = Objects.requireNonNull(payloadType, "payloadType must not be null");
        this.payloadSerializer = Objects.requireNonNull(payloadSerializer, "payloadSerializer must not be null");
    }

    @Override
    public byte[] serialize(SpoolEntryPOJO<T> entry) {
        Objects.requireNonNull(entry, "entry must not be null");
        byte[] keyBytes = entry.getRoutingKey().getBytes(StandardCharsets.UTF_8);
        byte[] payloadBytes = payloadSerializer.serialize(entry.getPayload());

        ByteBuffer buffer = ByteBuffer.allocate(4 + keyBytes.length + payloadBytes.length);
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.put(payloadBytes);
        return buffer.array();
    }

    @Override
    public SpoolEntryPOJO<T> deserialize(byte[] bytes, Class<SpoolEntryPOJO<T>> type) {
        Objects.requireNonNull(bytes, "bytes must not be null");
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int keyLen = buffer.getInt();
        byte[] keyBytes = new byte[keyLen];
        buffer.get(keyBytes);
        String routingKey = new String(keyBytes, StandardCharsets.UTF_8);

        byte[] payloadBytes = new byte[buffer.remaining()];
        buffer.get(payloadBytes);
        T payload = payloadSerializer.deserialize(payloadBytes, payloadType);

        return new SpoolEntryPOJO<>(routingKey, payload);
    }
}
