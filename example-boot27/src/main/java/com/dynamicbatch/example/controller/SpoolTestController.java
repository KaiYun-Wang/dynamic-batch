package com.dynamicbatch.example.controller;

import com.dynamicbatch.spool.Spool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Spool 演示 Controller：字符串消息 + 对象消息（验证默认 JDK 序列化）。
 *
 * <pre>
 * # 字符串消息
 * curl "http://localhost:8080/spool/append?msg=hello"
 * curl "http://localhost:8080/spool/poll"
 *
 * # 对象消息（含 map / list / 数组字段，走默认 JDK 序列化）
 * curl "http://localhost:8080/spool/append-obj?id=1&name=dev-1"
 * curl "http://localhost:8080/spool/poll-obj"
 *
 * # 锁语义演示（独立 spool，三个接口）：
 * curl "http://localhost:8080/spool/demo-read"    // ① 正常读：空队列立即返回 null（1ms 左右）
 * curl "http://localhost:8080/spool/demo-lock"    // ② 占住读锁（后台线程持有）
 * curl "http://localhost:8080/spool/demo-read"    // ③ 锁被占：等 5s 抛 TimeoutException
 * curl "http://localhost:8080/spool/demo-unlock"   // ④ 释放读锁
 * curl "http://localhost:8080/spool/demo-read"    // ⑤ 又能正常读了：立即返回 null
 * </pre>
 */
@RestController
@RequestMapping("/spool")
public class SpoolTestController {

    private static final Logger log = LoggerFactory.getLogger(SpoolTestController.class);

    /** 字符串消息目录与对象消息目录分开（目录锁要求一个目录只被一个 Spool 打开） */
    private static final Path STRING_DIR = Paths.get("spool-demo");
    private static final Path OBJ_DIR = Paths.get("spool-demo-obj");

    private Spool<String> stringSpool;
    private Spool<DeviceData> objSpool;
    /** 锁语义演示专用 spool，避免干扰上面两个 */
    private Spool<String> demoSpool;

    /** 持锁的后台线程（锁所有权在线程上，跨 HTTP 请求保持一致） */
    private volatile Thread lockHolderThread;

    @PostConstruct
    public void init() {
        // 不传序列化器 = 默认 JDK 原生（对象需实现 Serializable）
        stringSpool = Spool.builder(String.class, STRING_DIR)
                .stagingCapacity(5000)
                .flushIntervalMs(1000)
                .offerTimeoutMs(100)
                .rollCycleMillis(10000)
                .build();

        objSpool = Spool.builder(DeviceData.class, OBJ_DIR)
                .stagingCapacity(5000)
                .flushIntervalMs(1000)
                .offerTimeoutMs(100)
                .rollCycleMillis(10000)
                .build();

        demoSpool = Spool.builder(String.class, Paths.get("spool-demo-lock"))
                .stagingCapacity(5000)
                .flushIntervalMs(1000)
                .offerTimeoutMs(100)
                .rollCycleMillis(10000)
                .build();

        log.info("=== Spool demo started ===");
        log.info("字符串目录: {}  |  对象目录: {}",
                STRING_DIR.toAbsolutePath().normalize(), OBJ_DIR.toAbsolutePath().normalize());
    }

    @PreDestroy
    public void destroy() {
        if (stringSpool != null) stringSpool.close();
        if (objSpool != null) objSpool.close();
        if (demoSpool != null) demoSpool.close();
    }

    // ======================== 字符串消息 ========================

    /** 发一条字符串消息 */
    @GetMapping("/append")
    public String append(@RequestParam(defaultValue = "hello") String msg) {
        boolean ok = stringSpool.append(msg);
        return ok ? "OK, staging=" + stringSpool.stagingSize() : "FAIL（暂存队列满）";
    }

    /** 读一条字符串消息 */
    @GetMapping("/poll")
    public String poll() {
        try {
            String data = stringSpool.poll(1000);
            return data != null ? "读到: " + data : "队列为空";
        } catch (TimeoutException e) {
            return "读锁获取超时（并发读压力过大）";
        }
    }

    // ======================== 锁语义演示（三个接口） ========================

    /**
     * 演示读：锁等待 5 秒，空队列立即返回 null；锁被占则 5 秒后抛 TimeoutException。
     * 同一接口两种结果，配合 /demo-lock /demo-unlock 模拟完整流程。
     */
    @GetMapping("/demo-read")
    public String demoRead() {
        long start = System.currentTimeMillis();
        try {
            String data = demoSpool.poll(5000);
            long elapsed = System.currentTimeMillis() - start;
            return "elapsed=" + elapsed + "ms, result=" + (data == null ? "null（空队列立即返回）" : data);
        } catch (TimeoutException e) {
            long elapsed = System.currentTimeMillis() - start;
            return "elapsed=" + elapsed + "ms, TimeoutException: 获取锁超时（锁被占/并发读压力过大）";
        }
    }

    /** 占住读锁：启动一个后台线程持有锁，跨请求保持 */
    @GetMapping("/demo-lock")
    public String demoLock() {
        if (lockHolderThread != null) {
            return "锁已被占用";
        }
        try {
            ReentrantLock lock = getDemoReadLock();
            Thread holder = new Thread(() -> {
                lock.lock();
                // 持锁直到被 interrupt（/demo-unlock 触发）
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    lock.unlock();
                }
            }, "demo-lock-holder");
            holder.setDaemon(true);
            lockHolderThread = holder;
            holder.start();
            // 等后台线程真正拿到锁
            while (!lock.isLocked()) {
                Thread.yield();
            }
            return "读锁已占用（后台线程持有），去调 /demo-read 看锁超时";
        } catch (Exception e) {
            return "占锁失败: " + e;
        }
    }

    /** 释放读锁：中断后台持锁线程，让它在 finally 里释放 */
    @GetMapping("/demo-unlock")
    public String demoUnlock() {
        Thread holder = lockHolderThread;
        if (holder == null) {
            return "锁未被占用";
        }
        lockHolderThread = null;
        holder.interrupt();
        try {
            holder.join(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return "读锁已释放";
    }

    /** 反射拿 demoSpool 的读锁（仅演示用） */
    private ReentrantLock getDemoReadLock() throws Exception {
        Field readerField = Spool.class.getDeclaredField("reader");
        readerField.setAccessible(true);
        Object reader = readerField.get(demoSpool);
        Field lockField = reader.getClass().getDeclaredField("lock");
        lockField.setAccessible(true);
        return (ReentrantLock) lockField.get(reader);
    }

    // ======================== 对象消息（默认 JDK 序列化测试） ========================

    /**
     * 插入一个含 map / list / 数组字段的对象，验证默认 JDK 序列化落盘。
     * @param id    设备 ID
     * @param name  设备名
     */
    @GetMapping("/append-obj")
    public String appendObj(@RequestParam long id, @RequestParam String name) {
        DeviceData data = new DeviceData();
        data.setId(id);
        data.setName(name);

        Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("os", "linux");
        attrs.put("cores", 8);
        attrs.put("online", true);
        data.setAttributes(attrs);

        data.setTags(Arrays.asList("edge", "iot", "gw-" + id));
        data.setChannels(new String[]{"mqtt", "http", "kafka"});

        boolean ok = objSpool.append(data);
        return ok ? "OK, staging=" + objSpool.stagingSize() : "FAIL（暂存队列满）";
    }

    /** 读一条对象消息，直接返回对象（Spring 自动转 JSON） */
    @GetMapping("/poll-obj")
    public Object pollObj() {
        try {
            DeviceData data = objSpool.poll(1000);
            return data != null ? data : "队列为空";
        } catch (TimeoutException e) {
            return "读锁获取超时（并发读压力过大）";
        }
    }

    /**
     * 测试对象：含 map / list / 数组字段。
     * 实现 {@link Serializable} 以支持默认 JDK 序列化。
     */
    public static class DeviceData implements Serializable {
        private static final long serialVersionUID = 1L;

        private long id;
        private String name;
        private Map<String, Object> attributes;
        private List<String> tags;
        private String[] channels;

        public long getId() { return id; }
        public void setId(long id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public Map<String, Object> getAttributes() { return attributes; }
        public void setAttributes(Map<String, Object> attributes) { this.attributes = attributes; }
        public List<String> getTags() { return tags; }
        public void setTags(List<String> tags) { this.tags = tags; }
        public String[] getChannels() { return channels; }
        public void setChannels(String[] channels) { this.channels = channels; }
    }
}