package com.dynamicbatch.common.pojo;

import java.io.Serializable;
import java.util.Objects;

/**
 * 落盘条目：业务保序键 + 载荷，Spool 的元素类型（业务提交时由框架内部包装）。
 * JavaBean 形态以兼容常见序列化框架（兼容性由 SpoolEntryPOJOSerializationTest 矩阵锁定）。
 */
public final class SpoolEntryPOJO<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 业务保序键，路由/分区依据 */
    private String routingKey;
    /** 载荷 */
    private T payload;

    /** 供序列化框架实例化。 */
    public SpoolEntryPOJO() {
    }

    /**
     * @param routingKey 业务保序键，不可为 null
     * @param payload    载荷
     * @throws NullPointerException routingKey 为 null
     */
    public SpoolEntryPOJO(String routingKey, T payload) {
        this.routingKey = Objects.requireNonNull(routingKey, "routingKey must not be null");
        this.payload = payload;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public T getPayload() {
        return payload;
    }

    public void setPayload(T payload) {
        this.payload = payload;
    }

    /** 摘要形式，不含载荷内容。 */
    @Override
    public String toString() {
        return "SpoolEntryPOJO{routingKey=" + routingKey
                + ", payload=" + (payload == null ? "null" : payload.getClass().getName()) + '}';
    }
}
