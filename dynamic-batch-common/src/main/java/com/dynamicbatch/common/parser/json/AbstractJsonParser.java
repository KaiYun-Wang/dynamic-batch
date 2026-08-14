package com.dynamicbatch.common.parser.json;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JsonParser 抽象基类：基于 classpath 依赖探测的 supports() 模板实现。
 *
 * <p>子类只需声明运行所需的依赖类全限定名（{@link #getMapperClassNames()}），
 * supports() 逐个 {@link Class#forName} 探测，依赖缺失时返回 false 并记录 warn，
 * 使 {@link com.dynamicbatch.common.util.JsonUtil} 能自动跳过不可用的实现。
 * 参考 dromara dynamic-tp 的 AbstractJsonParser。
 */
public abstract class AbstractJsonParser implements JsonParser {

    private static final Logger log = LoggerFactory.getLogger(AbstractJsonParser.class);

    @Override
    public boolean supports() {
        for (String mapperClassName : getMapperClassNames()) {
            try {
                Class.forName(mapperClassName);
            } catch (ClassNotFoundException e) {
                log.warn("JsonParser {} 不可用，缺少依赖类: {}", getClass().getSimpleName(), mapperClassName);
                return false;
            }
        }
        return true;
    }

    /**
     * 本实现运行所需的依赖类全限定名。
     *
     * @return 依赖类名数组，全部存在于 classpath 时 supports() 才返回 true
     */
    protected abstract String[] getMapperClassNames();
}
