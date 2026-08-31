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
 * SpoolEntryPOJO 多序列化框架兼容矩阵：验证条目可被常见序列化方式直接编解码。
 *
 * <p>背景：条目由框架信封序列化器落盘（payload 编解码委托使用方序列化器），但类形态本身
 * 必须对常见序列化框架全兼容——无论信封实现如何演进、条目以何种方式被外部框架处理都不踩坑。
 * 本测试即形态契约：改类结构时此矩阵必须全绿。
 *
 * <p>覆盖：JDK 原生 / Kryo / Jackson(JSON) / MessagePack(jackson 后端) / Fastjson。
 * payload 泛型字段用 TypeReference 在测试侧绑定具体类型（框架处理泛型字段的标准做法）。
 */
public class SpoolEntryPOJOSerializationTest {

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

    private static SpoolEntryPOJO<Payload> sample() {
        return new SpoolEntryPOJO<>("device-1", new Payload(42L, "gw-42"));
    }

    /** 往返后字段一致性断言（各框架共用） */
    private static void assertRoundTripped(SpoolEntryPOJO<Payload> back) {
        assertEquals("device-1", back.getRoutingKey());
        assertTrue("载荷类型应保持", back.getPayload() instanceof Payload);
        assertEquals(42L, back.getPayload().getId());
        assertEquals("gw-42", back.getPayload().getName());
    }

    @Test
    public void jdk() throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(sample());
        }
        SpoolEntryPOJO<Payload> back;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            back = (SpoolEntryPOJO<Payload>) ois.readObject();
        }
        assertRoundTripped(back);
    }

    @Test
    public void kryo() {
        Kryo kryo = new Kryo();
        kryo.setRegistrationRequired(false);
        kryo.register(SpoolEntryPOJO.class);
        kryo.register(Payload.class);

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Output output = new Output(bos);
        kryo.writeObject(output, sample());
        output.close();

        Input input = new Input(new ByteArrayInputStream(bos.toByteArray()));
        @SuppressWarnings("unchecked")
        SpoolEntryPOJO<Payload> back = (SpoolEntryPOJO<Payload>) kryo.readObject(input, SpoolEntryPOJO.class);
        input.close();

        assertRoundTripped(back);
    }

    @Test
    public void jackson() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(sample());
        SpoolEntryPOJO<Payload> back = mapper.readValue(
                json, new com.fasterxml.jackson.core.type.TypeReference<SpoolEntryPOJO<Payload>>() {});
        assertRoundTripped(back);
    }

    @Test
    public void messagePack() throws Exception {
        ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());
        byte[] bytes = mapper.writeValueAsBytes(sample());
        SpoolEntryPOJO<Payload> back = mapper.readValue(
                bytes, new com.fasterxml.jackson.core.type.TypeReference<SpoolEntryPOJO<Payload>>() {});
        assertRoundTripped(back);
    }

    @Test
    public void fastJson() {
        String json = JSON.toJSONString(sample());
        SpoolEntryPOJO<Payload> back = JSON.parseObject(
                json, new TypeReference<SpoolEntryPOJO<Payload>>() {});
        assertRoundTripped(back);
    }
}
