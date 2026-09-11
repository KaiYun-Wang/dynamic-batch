package com.dynamicbatch.core.serializer;

import com.dynamicbatch.common.pojo.EnvelopePOJO;
import com.dynamicbatch.spool.Serializer;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * 信封序列化器测试：帧格式锁定 + round-trip + 坏帧契约。
 */
public class EnvelopeSerializerTest {

    private static Serializer<String> utf8PayloadSerializer() {
        return new Serializer<String>() {
            @Override
            public byte[] serialize(String data) {
                return data.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public String deserialize(byte[] bytes, Class<String> type) {
                return new String(bytes, StandardCharsets.UTF_8);
            }
        };
    }

    /**
     * 肉眼查看落盘字节：去掉 {@link Ignore} 后单独运行本方法。
     * {@code mvn -pl dynamic-batch-core test -Dtest=EnvelopeSerializerTest#frameHexDumpDemo}
     */
    @Ignore("手动运行看序列化效果，不纳入常规 CI")
    @Test
    public void frameHexDumpDemo() {
        EnvelopeSerializer<String> serializer =
                new EnvelopeSerializer<>(String.class, utf8PayloadSerializer());
        EnvelopePOJO<String> envelope = new EnvelopePOJO<>("device-1", "hello", 1725840000123L);
        byte[] bytes = serializer.serialize(envelope);

        System.out.println("输入: routingKey=\"" + envelope.getRoutingKey()
                + "\", submitTimeMillis=" + envelope.getSubmitTimeMillis()
                + ", payload=\"" + envelope.getPayload() + "\"");
        System.out.println("总长度: " + bytes.length + " 字节\n");

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int keyLen = buffer.getInt();
        System.out.println("① key 长度 (4B): " + toHex(bytes, 0, 4) + "  → int = " + keyLen);
        System.out.println("② routingKey (" + keyLen + "B): " + toHex(bytes, 4, keyLen)
                + "  → \"" + new String(bytes, 4, keyLen, StandardCharsets.UTF_8) + "\"");
        System.out.println("③ submitTimeMillis (8B): " + toHex(bytes, 4 + keyLen, 8)
                + "  → long = " + buffer.getLong(4 + keyLen));
        int payloadOffset = 4 + keyLen + 8;
        System.out.println("④ payload (" + (bytes.length - payloadOffset) + "B): "
                + toHex(bytes, payloadOffset, bytes.length - payloadOffset)
                + "  → \"" + new String(bytes, payloadOffset, bytes.length - payloadOffset, StandardCharsets.UTF_8) + "\"");
        System.out.println("\n完整报文: " + toHex(bytes, 0, bytes.length));
    }

    private static String toHex(byte[] bytes, int offset, int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = offset; i < offset + length; i++) {
            if (i > offset) {
                sb.append(' ');
            }
            sb.append(String.format("%02X", bytes[i]));
        }
        return sb.toString();
    }

    @Test
    public void frameLayoutLocks() {
        long submitAt = 1725840000123L;
        EnvelopeSerializer<String> serializer =
                new EnvelopeSerializer<>(String.class, utf8PayloadSerializer());
        byte[] bytes = serializer.serialize(new EnvelopePOJO<>("k1", "v1", submitAt));

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        assertEquals(2, buffer.getInt());
        byte[] keyBytes = new byte[2];
        buffer.get(keyBytes);
        assertArrayEquals("k1".getBytes(StandardCharsets.UTF_8), keyBytes);
        assertEquals("帧第③段应为 8 字节提交时间戳", submitAt, buffer.getLong());
        byte[] payloadBytes = new byte[buffer.remaining()];
        buffer.get(payloadBytes);
        assertArrayEquals("v1".getBytes(StandardCharsets.UTF_8), payloadBytes);
    }

    @Test
    public void roundTrip() {
        AtomicInteger serializeCount = new AtomicInteger();
        AtomicInteger deserializeCount = new AtomicInteger();
        Serializer<String> countingPayloadSerializer = new Serializer<String>() {
            @Override
            public byte[] serialize(String data) {
                serializeCount.incrementAndGet();
                return data.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public String deserialize(byte[] bytes, Class<String> type) {
                deserializeCount.incrementAndGet();
                return new String(bytes, StandardCharsets.UTF_8);
            }
        };

        EnvelopeSerializer<String> serializer =
                new EnvelopeSerializer<>(String.class, countingPayloadSerializer);
        EnvelopePOJO<String> original = new EnvelopePOJO<>("route-a", "payload-a");
        EnvelopePOJO<String> back = serializer.deserialize(serializer.serialize(original), null);

        assertEquals("route-a", back.getRoutingKey());
        assertEquals("payload-a", back.getPayload());
        assertEquals("提交时间戳应往返保持", original.getSubmitTimeMillis(), back.getSubmitTimeMillis());
        assertEquals(1, serializeCount.get());
        assertEquals(1, deserializeCount.get());
    }

    @Test
    public void multiByteKeyRoundTrip() {
        EnvelopeSerializer<String> serializer =
                new EnvelopeSerializer<>(String.class, utf8PayloadSerializer());
        EnvelopePOJO<String> original = new EnvelopePOJO<>("设备-1", "data");
        EnvelopePOJO<String> back = serializer.deserialize(serializer.serialize(original), null);

        assertEquals("设备-1", back.getRoutingKey());
        assertEquals("data", back.getPayload());
        assertEquals("提交时间戳应往返保持", original.getSubmitTimeMillis(), back.getSubmitTimeMillis());
    }

    @Test
    public void corruptedFrameThrowsRuntime() {
        EnvelopeSerializer<String> serializer =
                new EnvelopeSerializer<>(String.class, utf8PayloadSerializer());
        byte[] truncated = new byte[]{0, 0, 0, 2, 'k'};
        try {
            serializer.deserialize(truncated, null);
            fail("expected RuntimeException");
        } catch (RuntimeException expected) {
            // Spool 或 payload 序列化器抛出的运行时异常族
        }

        byte[] badKeyLen = ByteBuffer.allocate(4).putInt(-1).array();
        try {
            serializer.deserialize(badKeyLen, null);
            fail("expected exception");
        } catch (Exception expected) {
            assertTrue(true);
        }
    }

    @Test
    public void absurdKeyLenFailsFast() {
        // 荒谬 keyLen（如被截断/错位帧误读出超大值）必须在分配前快速失败，防其放大成 OOM
        EnvelopeSerializer<String> serializer =
                new EnvelopeSerializer<>(String.class, utf8PayloadSerializer());
        byte[] absurd = ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE).array();
        try {
            serializer.deserialize(absurd, null);
            fail("absurd key length should fail fast");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("out of range"));
        }
    }
}
