package com.dynamicbatch.core.notifier.context;

/**
 * 通知事件上下文基类：携带所有消息类型通用的数据。
 *
 * <p>具体场景的上下文继承本类并扩展各自字段（如变更场景的 {@link ChangeContext}），
 * 由 {@code NotifyManager} 在场景入口方法内部构造，模板从上下文取自己需要的字段。
 * 设计参考 dromara dynamic-tp 的 core/notifier/context 包（BaseNotifyCtx + 场景子类）。
 */
public abstract class NotifyContext {

    private final String key;

    protected NotifyContext(String key) {
        this.key = key;
    }

    /** worker 唯一标识 */
    public String getKey() {
        return key;
    }
}
