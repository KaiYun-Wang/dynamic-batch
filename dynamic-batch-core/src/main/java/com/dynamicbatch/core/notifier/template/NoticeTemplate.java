package com.dynamicbatch.core.notifier.template;

import com.dynamicbatch.common.enums.NotifyTypeEnum;
import com.dynamicbatch.core.notifier.context.NotifyContext;

/**
 * 通知模板接口：负责按消息类型构建通知内容。
 *
 * <p>实现类通过 {@link #type()} 声明自己支持的通知类型，由 {@code NotifyManager}
 * 注册表按类型分发；泛型参数 {@code C} 绑定本模板对应的场景上下文（如变更场景的
 * {@code ChangeContext}），保证 {@link #build} 拿到的就是本类型需要的字段。
 * 模板只负责"事件数据 → 消息内容"，并自行决定"本次是否要发"：返回 {@code null}
 * 表示无需发送（如变更模板对比新旧配置无差异时），门面直接跳过，不打扰渠道层。
 *
 * @param <C> 本模板对应的场景上下文类型
 */
public interface NoticeTemplate<C extends NotifyContext> {

    /**
     * 本模板支持的通知类型，作为注册表 key。
     *
     * @return 通知类型
     */
    NotifyTypeEnum type();

    /**
     * 构建通知内容。
     *
     * @param context 场景上下文（由 NotifyManager 场景入口构造并投递）
     * @return 消息内容（markdown），{@code null} 表示本次无需发送
     */
    String build(C context);
}
