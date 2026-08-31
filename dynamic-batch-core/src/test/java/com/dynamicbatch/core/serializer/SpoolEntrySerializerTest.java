package com.dynamicbatch.core.serializer;

import com.dynamicbatch.common.pojo.SpoolEntryPOJO;
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
public class SpoolEntrySerializerTest {

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
     * {@code mvn -pl dynamic-batch-core test -Dtest=SpoolEntrySerializerTest#frameHexDumpDemo}
     */
    @Ignore("手动运行看序列化效果，不纳入常规 CI")
    @Test
    public void frameHexDumpDemo() {
        SpoolEntrySerializer<String> serializer =
                new SpoolEntrySerializer<>(String.class, utf8PayloadSerializer());
        SpoolEntryPOJO<String> entry = new SpoolEntryPOJO<>("device-1", "hello");
        byte[] bytes = serializer.serialize(entry);

        System.out.println("输入: routingKey=\"" + entry.getRoutingKey() + "\", payload=\"" + entry.getPayload() + "\"");
        System.out.println("总长度: " + bytes.length + " 字节\n");

        int keyLen = ByteBuffer.wrap(bytes, 0, 4).getInt();
        System.out.println("① key 长度 (4B): " + toHex(bytes, 0, 4) + "  → int = " + keyLen);
        System.out.println("② routingKey (" + keyLen + "B): " + toHex(bytes, 4, keyLen)
                + "  → \"" + new String(bytes, 4, keyLen, StandardCharsets.UTF_8) + "\"");
        System.out.println("③ payload (" + (bytes.length - 4 - keyLen) + "B): "
                + toHex(bytes, 4 + keyLen, bytes.length - 4 - keyLen)
                + "  → \"" + new String(bytes, 4 + keyLen, bytes.length - 4 - keyLen, StandardCharsets.UTF_8) + "\"");
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
        SpoolEntrySerializer<String> serializer =
                new SpoolEntrySerializer<>(String.class, utf8PayloadSerializer());
        byte[] bytes = serializer.serialize(new SpoolEntryPOJO<>("k1", "v1"));

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        assertEquals(2, buffer.getInt());
        byte[] keyBytes = new byte[2];
        buffer.get(keyBytes);
        assertArrayEquals("k1".getBytes(StandardCharsets.UTF_8), keyBytes);
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

        SpoolEntrySerializer<String> serializer =
                new SpoolEntrySerializer<>(String.class, countingPayloadSerializer);
        SpoolEntryPOJO<String> original = new SpoolEntryPOJO<>("route-a", "payload-a");
        SpoolEntryPOJO<String> back = serializer.deserialize(serializer.serialize(original), null);

        assertEquals("route-a", back.getRoutingKey());
        assertEquals("payload-a", back.getPayload());
        assertEquals(1, serializeCount.get());
        assertEquals(1, deserializeCount.get());
    }

    @Test
    public void multiByteKeyRoundTrip() {
        SpoolEntrySerializer<String> serializer =
                new SpoolEntrySerializer<>(String.class, utf8PayloadSerializer());
        SpoolEntryPOJO<String> original = new SpoolEntryPOJO<>("设备-1", "data");
        SpoolEntryPOJO<String> back = serializer.deserialize(serializer.serialize(original), null);

        assertEquals("设备-1", back.getRoutingKey());
        assertEquals("data", back.getPayload());
    }

    @Test
    public void corruptedFrameThrowsRuntime() {
        SpoolEntrySerializer<String> serializer =
                new SpoolEntrySerializer<>(String.class, utf8PayloadSerializer());
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
}
