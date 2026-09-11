package com.dynamicbatch.common.pojo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * EnvelopePOJO 多序列化框架兼容矩阵：验证信封可被常见序列化方式直接编解码。
 *
 * <p>背景：信封由框架序列化器落盘（payload 编解码委托使用方序列化器），但类形态本身
 * 必须对常见序列化框架全兼容——无论信封实现如何演进、信封以何种方式被外部框架处理都不踩坑。
 * 本测试即形态契约：改类结构时此矩阵必须全绿。
 *
 * <p>覆盖：JDK 原生 / Kryo / Jackson(JSON) / MessagePack(jackson 后端) / Fastjson。
 * payload 泛型字段用 TypeReference 在测试侧绑定具体类型（框架处理泛型字段的标准做法）。
 */
public class EnvelopePOJOSerializationTest {

    /** 测试用固定提交时间戳（epoch 毫秒），三参构造器写入以锁定字段往返 */
    private static final long SUBMIT_AT = 1725840000123L;

    /**
     * 测试载荷：模拟业务 data 类的标准形态（JavaBean）——用户选 JSON/MessagePack 等序列化方式时，
     * 自己的 data 类本就应满足对应框架的形态要求（本类取最大公约数形态）。
     */
    static class Payload implements Serializable {
        private static final long serialVersionUID = 1L;
        private long id;
        private String name;

        public Payload() {
        }

        public Payload(long id, String name) {
            this.id = id;
            this.name = name;
        }

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    private static EnvelopePOJO<Payload> sample() {
        return new EnvelopePOJO<>("device-1", new Payload(42L, "gw-42"), SUBMIT_AT);
    }

    /** 往返后字段一致性断言（各框架共用） */
    private static void assertRoundTripped(EnvelopePOJO<Payload> back) {
        assertEquals("device-1", back.getRoutingKey());
        assertTrue("载荷类型应保持", back.getPayload() instanceof Payload);
        assertEquals(42L, back.getPayload().getId());
        assertEquals("gw-42", back.getPayload().getName());
        assertEquals("提交时间戳应往返保持", SUBMIT_AT, back.getSubmitTimeMillis());
    }

    @Test
    public void jdk() throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(sample());
        }
        EnvelopePOJO<Payload> back;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            back = (EnvelopePOJO<Payload>) ois.readObject();
        }
        assertRoundTripped(back);
    }

    @Test
    public void kryo() {
        Kryo kryo = new Kryo();
        kryo.setRegistrationRequired(false);
        kryo.register(EnvelopePOJO.class);
        kryo.register(Payload.class);

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Output output = new Output(bos);
        kryo.writeObject(output, sample());
        output.close();

        Input input = new Input(new ByteArrayInputStream(bos.toByteArray()));
        @SuppressWarnings("unchecked")
        EnvelopePOJO<Payload> back = (EnvelopePOJO<Payload>) kryo.readObject(input, EnvelopePOJO.class);
        input.close();

        assertRoundTripped(back);
    }

    @Test
    public void jackson() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(sample());
        EnvelopePOJO<Payload> back = mapper.readValue(
                json, new com.fasterxml.jackson.core.type.TypeReference<EnvelopePOJO<Payload>>() {});
        assertRoundTripped(back);
    }

    @Test
    public void messagePack() throws Exception {
        ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());
        byte[] bytes = mapper.writeValueAsBytes(sample());
        EnvelopePOJO<Payload> back = mapper.readValue(
                bytes, new com.fasterxml.jackson.core.type.TypeReference<EnvelopePOJO<Payload>>() {});
        assertRoundTripped(back);
    }

    @Test
    public void fastJson() {
        String json = JSON.toJSONString(sample());
        EnvelopePOJO<Payload> back = JSON.parseObject(
                json, new TypeReference<EnvelopePOJO<Payload>>() {});
        assertRoundTripped(back);
    }
}
