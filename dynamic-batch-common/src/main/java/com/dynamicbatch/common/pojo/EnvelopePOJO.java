package com.dynamicbatch.common.pojo;

import java.io.Serializable;
import java.util.Objects;

/**
 * 提交链路内部信封：业务保序键 + 载荷 + 提交时间戳，提交时由框架内部包装。
 * JavaBean 形态以兼容常见序列化框架（兼容性由 EnvelopePOJOSerializationTest 矩阵锁定）。
 */
public final class EnvelopePOJO<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 业务保序键，路由/分区依据 */
    private String routingKey;
    /** 载荷 */
    private T payload;
    /** 进入提交链路时刻（epoch 毫秒）；0 = 未知哨兵（不计 RT） */
    private long submitTimeMillis;

    /** 供序列化框架实例化。 */
    public EnvelopePOJO() {
    }

    /**
     * 提交路径构造器：以当前时刻打提交时间戳。
     *
     * @param routingKey 业务保序键，不可为 null
     * @param payload    载荷
     * @throws NullPointerException routingKey 为 null
     */
    public EnvelopePOJO(String routingKey, T payload) {
        this(routingKey, payload, System.currentTimeMillis());
    }

    /**
     * 还原构造器：落盘往返后按帧中保存的戳重建，不重新打戳。
     *
     * @param routingKey       业务保序键，不可为 null
     * @param payload          载荷
     * @param submitTimeMillis 落盘时保存的提交时间戳
     * @throws NullPointerException routingKey 为 null
     */
    public EnvelopePOJO(String routingKey, T payload, long submitTimeMillis) {
        this.routingKey = Objects.requireNonNull(routingKey, "routingKey must not be null");
        this.payload = payload;
        this.submitTimeMillis = submitTimeMillis;
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

    public long getSubmitTimeMillis() {
        return submitTimeMillis;
    }

    public void setSubmitTimeMillis(long submitTimeMillis) {
        this.submitTimeMillis = submitTimeMillis;
    }

    /** 摘要形式，不含载荷内容。 */
    @Override
    public String toString() {
        return "EnvelopePOJO{routingKey=" + routingKey
                + ", payload=" + (payload == null ? "null" : payload.getClass().getName()) + '}';
    }
}
