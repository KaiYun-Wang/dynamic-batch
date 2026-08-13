package com.dynamicbatch.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JDK ServiceLoader 封装工具，提供缓存与容错。
 *
 * <p>{@link ServiceLoader} 的简单封装：接口 + META-INF/services 注册实现类，
 * 可在不修改核心代码的前提下替换 / 扩展某个 SPI 接口的实现。
 * 借鉴 dromara dynamic-tp ExtensionServiceLoader。
 */
public class ExtensionServiceLoader {

    private static final Logger log = LoggerFactory.getLogger(ExtensionServiceLoader.class);

    private static final Map<Class<?>, List<?>> EXTENSION_MAP = new ConcurrentHashMap<>();

    private ExtensionServiceLoader() {
    }

    /**
     * 加载指定 SPI 接口的所有实现。
     *
     * @param clazz SPI 接口
     * @param <T>   接口类型
     * @return 实现列表（可能为空）
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> get(Class<T> clazz) {
        List<T> services = (List<T>) EXTENSION_MAP.get(clazz);
        if (services == null) {
            services = load(clazz);
            if (services != null) {
                EXTENSION_MAP.put(clazz, services);
            }
        }
        return services;
    }

    /**
     * 加载第一个可用的实现。
     *
     * @param clazz SPI 接口
     * @param <T>   接口类型
     * @return 第一个实现，无实现返回 null
     */
    public static <T> T getFirst(Class<T> clazz) {
        List<T> services = get(clazz);
        return (services == null || services.isEmpty()) ? null : services.get(0);
    }

    private static <T> List<T> load(Class<T> clazz) {
        ServiceLoader<T> serviceLoader = ServiceLoader.load(clazz);
        Iterator<T> iterator = serviceLoader.iterator();
        List<T> services = new ArrayList<>();
        while (true) {
            try {
                if (!iterator.hasNext()) {
                    break;
                }
            } catch (ServiceConfigurationError e) {
                log.warn("Failed to load {} provider, skip remaining.", clazz.getName(), e);
                break;
            }
            try {
                services.add(iterator.next());
            } catch (ServiceConfigurationError e) {
                log.warn("Failed to load {} provider, skip it.", clazz.getName(), e);
            }
        }
        return services;
    }
}