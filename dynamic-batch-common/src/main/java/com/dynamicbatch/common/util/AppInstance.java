package com.dynamicbatch.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

/**
 * 应用实例信息访问器，参考 dromara dynamic-tp 的 {@code CommonUtil} + {@code ServiceInstance}。
 *
 * <p>职责：提供当前实例的 IP、应用名、端口。IP 在类加载时通过纯 JDK API 获取，
 * 不依赖任何第三方库；应用名和端口由 Spring 层启动时通过 {@link #init} 注入。
 *
 * <ul>
 *   <li>IP 获取算法：优先返回非回环、站点本地地址的第一块非 P2P 网卡 IP；
 *       无符合条件的站点本地地址时回退到 {@link InetAddress#getLocalHost()}。</li>
 *   <li>应用名 / 端口：初始均为 "unknown" / 0，Spring 层调 {@link #init} 覆盖。</li>
 * </ul>
 */
public final class AppInstance {

    private static final Logger log = LoggerFactory.getLogger(AppInstance.class);

    private static final String IP;
    private static String appName = "unknown";
    private static int port;

    static {
        String ip;
        try {
            ip = resolveLocalIp();
        } catch (Exception e) {
            log.error("resolve local ip failed", e);
            ip = "unknown";
        }
        IP = ip;
    }

    private AppInstance() {
    }

    /**
     * 由 Spring 层启动时调用，注入应用名和端口。
     *
     * @param appName 应用名（spring.application.name）
     * @param port    端口（server.port）
     */
    public static void init(String appName, int port) {
        AppInstance.appName = appName != null ? appName : "unknown";
        AppInstance.port = port;
        log.debug("AppInstance initialized, ip={}, appName={}, port={}", IP, appName, port);
    }

    public static String getIp() {
        return IP;
    }

    public static String getAppName() {
        return appName;
    }

    public static int getPort() {
        return port;
    }

    /** 实例标识，格式 "ip:port (appName)"，方便通知内容直接引用 */
    public static String instanceLabel() {
        return port > 0 ? IP + ":" + port + " (" + appName + ")" : IP + " (" + appName + ")";
    }

    /**
     * 获取本地非回环、站点本地地址，算法与 dynamic-tp {@code CommonUtil.getLocalHostExactAddress()} 一致。
     */
    private static String resolveLocalIp() throws SocketException, UnknownHostException {
        InetAddress candidateAddress = null;
        Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        while (networkInterfaces.hasMoreElements()) {
            NetworkInterface networkInterface = networkInterfaces.nextElement();
            for (Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                 inetAddresses.hasMoreElements(); ) {
                InetAddress inetAddress = inetAddresses.nextElement();
                if (!inetAddress.isLoopbackAddress() && inetAddress.isSiteLocalAddress()) {
                    if (!networkInterface.isPointToPoint()) {
                        return inetAddress.getHostAddress();
                    } else {
                        candidateAddress = inetAddress;
                    }
                }
            }
        }
        InetAddress fallback = candidateAddress == null ? InetAddress.getLocalHost() : candidateAddress;
        return fallback.getHostAddress();
    }
}