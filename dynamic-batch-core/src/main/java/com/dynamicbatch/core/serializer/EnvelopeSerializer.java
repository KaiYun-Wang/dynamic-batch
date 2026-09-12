package com.dynamicbatch.core.serializer;

import com.dynamicbatch.common.pojo.EnvelopePOJO;
import com.dynamicbatch.spool.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Spool 落盘序列化器：把 {@link EnvelopePOJO} 拼成一段 byte[]，从左到右四块：
 * <pre>
 * ┌────────────────┬─────────────────────┬────────────────────┬──────────────────────────┐
 * │ ① key 字节长度   │ ② routingKey 二进制   │ ③ submitTimeMillis │ ④ payload 序列化结果      │
 * │   (4 字节 int)  │   (UTF-8)           │   (8 字节 long)     │   (用户 Serializer 决定)  │
 * └────────────────┴─────────────────────┴────────────────────┴──────────────────────────┘
 * </pre>
 * serialize 按此拼接，deserialize 按此拆开。帧格式改动会破坏已落盘数据。
 */
public class EnvelopeSerializer<T> implements Serializer<EnvelopePOJO<T>> {

    /** key 字节长度上限，超过视为坏块快速失败 */
    private static final int KEY_LEN_UPPER_BOUND = 64 * 1024;

    private final Class<T> payloadType;
    private final Serializer<T> payloadSerializer;

    public EnvelopeSerializer(Class<T> payloadType, Serializer<T> payloadSerializer) {
        this.payloadType = Objects.requireNonNull(payloadType, "payloadType must not be null");
        this.payloadSerializer = Objects.requireNonNull(payloadSerializer, "payloadSerializer must not be null");
    }

    @Override
    public byte[] serialize(EnvelopePOJO<T> envelope) {
        Objects.requireNonNull(envelope, "envelope must not be null");
        byte[] keyBytes = envelope.getRoutingKey().getBytes(StandardCharsets.UTF_8);
        byte[] payloadBytes = payloadSerializer.serialize(envelope.getPayload());

        ByteBuffer buffer = ByteBuffer.allocate(4 + keyBytes.length + 8 + payloadBytes.length);
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putLong(envelope.getSubmitTimeMillis());
        buffer.put(payloadBytes);
        return buffer.array();
    }

    @Override
    public EnvelopePOJO<T> deserialize(byte[] bytes, Class<EnvelopePOJO<T>> type) {
        Objects.requireNonNull(bytes, "bytes must not be null");
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int keyLen = buffer.getInt();
        if (keyLen < 0 || keyLen > KEY_LEN_UPPER_BOUND) {
            throw new IllegalArgumentException("corrupted frame: key length " + keyLen + " out of range");
        }
        byte[] keyBytes = new byte[keyLen];
        buffer.get(keyBytes);
        String routingKey = new String(keyBytes, StandardCharsets.UTF_8);

        long submitTimeMillis = buffer.getLong();

        byte[] payloadBytes = new byte[buffer.remaining()];
        buffer.get(payloadBytes);
        T payload = payloadSerializer.deserialize(payloadBytes, payloadType);

        return new EnvelopePOJO<>(routingKey, payload, submitTimeMillis);
    }
}
