package com.dynamicbatch.common.pojo;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;

/**
 * SpoolEntryPOJO 序列化往返测试。
 *
 * <p>用裸 JDK ObjectStream 而非真 Spool：common 不能为测试依赖 spool（依赖方向反了，
 * spool 是可独立复用的下游模块），且裸流与 JdkSerializer 的实现
 * （{@code ObjectOutputStream.writeObject} / {@code ObjectInputStream.readObject}）逐字等价。
 */
public class SpoolEntryPojoTest {

    /** 测试载荷：模拟业务 DTO */
    static class Payload implements Serializable {
        private static final long serialVersionUID = 1L;
        final long id;
        final String name;

        Payload(long id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    /** 与 Spool 默认 JdkSerializer 相同的 JDK 序列化路径 */
    @SuppressWarnings("unchecked")
    private static <T> T roundTrip(T data) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(data);
        }
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            return (T) ois.readObject();
        }
    }

    @Test
    public void roundTrip() throws Exception {
        SpoolEntryPOJO<Payload> entry = new SpoolEntryPOJO<>("device-1", new Payload(42L, "gw-42"));
        SpoolEntryPOJO<Payload> back = roundTrip(entry);

        assertEquals("device-1", back.getRoutingKey());
        assertTrue("载荷类型应保持", back.getPayload() instanceof Payload);
        assertEquals(42L, back.getPayload().id);
        assertEquals("gw-42", back.getPayload().name);
    }

    @Test
    public void routingKeyMustNotBeNull() {
        // routingKey 不可为 null：null 无法取模路由（null.hashCode() 直接 NPE），入口 fail fast；
        // 无键提交场景由 Processor 生成随机键（规划中）
        try {
            new SpoolEntryPOJO<>(null, new Payload(1L, "gw-1"));
            fail("routingKey 为 null 应拋 NullPointerException");
        } catch (NullPointerException e) {
            assertEquals("routingKey must not be null", e.getMessage());
        }
    }

    @Test
    public void toStringSummary() {
        SpoolEntryPOJO<Payload> entry = new SpoolEntryPOJO<>("device-1", new Payload(42L, "gw-42"));
        String s = entry.toString();

        assertTrue("toString 应含 routingKey", s.contains("device-1"));
        assertTrue("toString 应含载荷类名", s.contains("Payload"));
        assertFalse("toString 不应含载荷内容（只允许类名）", s.contains("gw-42"));
    }
}
