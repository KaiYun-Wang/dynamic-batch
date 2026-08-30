# dynamic-batch-spool

磁盘级 FIFO 缓冲队列（Spool）：基于 [Chronicle-Queue](https://github.com/OpenHFT/Chronicle-Queue) 构建，
解决了原生 Chronicle-Queue 在多线程场景下的三个核心痛点。

> Chronicle-Queue 的**单个 Appender / Tailer 实例是线程不安全的**（`@SingleThreaded`），原生要求每个线程持有独立实例；若要共享实例，需使用者自行加锁。且不提供已消费文件的自动清理、不提供写缓冲，数据持久化完全依赖操作系统默认的异步刷盘周期（最长 ~30 秒）。
>
> Spool 在此之上封装了**并发安全**、**自动清理**、**持久化窗口收窄**，让 Chronicle-Queue 可以开箱即用于多线程生产-消费场景。

## 对比原生 Chronicle-Queue

| 能力 | 原生 Chronicle-Queue | Spool |
|------|---------------------|-------|
| **并发安全** | Appender/Tailer 均线程不安全（`@SingleThreaded`） | 任意线程可并发 append/poll，零锁入队、锁内串行读 |
| **数据持久化** | mmap 写入后依赖 OS 异步回写（page cache，最长 ~30 秒） | 定时 sync 收窄丢失窗口到 `flushIntervalMs`（默认 5 秒） |
| **读位置持久化** | 命名 tailer 持久化到 mmap，同样依赖 OS 回写 | 和数据 sync 同周期主动刷盘，重复消费窗口收窄到 `flushIntervalMs` |
| **已消费文件清理** | 不提供自动删除 | 后台定时清理，tailer 读完即删，磁盘空间有界 |
| **写缓冲** | appender.writeBytes() 直写 mmap，生产者阻塞 | 有界暂存队列承接突发写入，生产者微秒级返回 |
| **多进程防护** | 无 | 目录独占锁，同目录第二个进程直接 fail fast |

## 核心作用

```mermaid
flowchart LR
    P["生产者"] --> SQ["暂存队列（内存）"]
    SQ --> WT["写线程"]
    WT --> CQ["Chronicle-Queue（磁盘）"]
    CQ -->|poll 锁内串行| C["消费者"]
```

- **写路径零锁**：生产者只入有界暂存队列（微秒级），后台单写线程串行落盘
- **读路径单锁**：任意线程可并发 poll，锁内串行化共享 tailer，不重复不遗漏
- **磁盘即容量**：消费暂停时数据堆积在磁盘，不占内存；可用 `maxSizeBytes` 限制目录占用
- **可独立复用**：模块零 Spring 依赖，可整体拷到其他项目当缓冲池

## 工作原理

### 写路径

`append(T)` 先看磁盘预算缓存：`currentSizeBytes >= maxSizeBytes` 则直接返回 false；
再序列化并进入有界暂存队列（`LinkedBlockingQueue`）。返回 true 仅代表**已进入内存暂存队列**。
后台单写线程批量取出（最多 128 条/批）写入 Chronicle-Queue（mmap 追加写，顺序 IO 高效）。

目录占用由 `refreshDiskUsage()` 扫盘更新：启动时一次、`dirSizeRefreshIntervalMs` 定时刷新、
清理旧文件后也会刷新。Chronicle 按块预留（常见约 80MB/块），空队列也可能已有较大占用。

### 读路径

`poll(lockTimeoutMs)` 用一把非公平 `ReentrantLock` 串行化共享 tailer（命名 tailer `spool-tailer`），
锁内读取并推进游标。语义：**lockTimeoutMs 是获取锁的最长等待**；拿到锁后读一次——有数据返回，
队列空**立即返回 null**（不等待）。**锁超时抛 `TimeoutException`**，调用方据此区分"队列空"与"并发读过载"。
**poll 即交付**：取出即视为调用方职责，无 ack/commit 机制。

### 文件滚动

按时间滚动（默认 `FIVE_MINUTELY` 5 分钟，可用 `rollCycleMillis()` 指定任意毫秒数）。
每个周期一个独立 `.cq4` 文件，文件名 = 该周期起始时间的格式化字符串。

### 持久化机制

```
append() → mmap 写入 page cache（内核管理, JVM 无关） → OS 内核线程异步刷盘（默认最长 ~30 秒）
                                                                       ↑
                                                              定时 sync 强制刷盘
```

Chronicle-Queue 使用内存映射文件（mmap），数据写入后首先到达**操作系统内核的 page cache**，然后由 OS 内核线程在后台异步回写到磁盘。这意味着：

- **kill -9 不丢数据**：JVM 进程被杀，OS 仍然在运行，page cache 还在，数据最终会刷下去
- **断电/OS 崩溃才会丢**：机房断电、系统蓝屏，page cache 里的数据还没到磁盘就没了
- **OS 默认丢失窗口 ~30 秒**（`vm.dirty_expire_centisecs`），但 SSD 通常提前刷了

Spool 的定时 sync 把这个窗口从 ~30 秒收窄到 `flushIntervalMs`（默认 5 秒），同时**读位置（offset）也在同一周期 sync**，数据丢失和重复消费的范围保持一致。

### 文件删除

`SpoolTimer` 后台线程定期（默认 = 滚动周期，可用 `cleanupIntervalMs()` 单独指定）调用清理逻辑：
以 tailer 当前正在读的文件（`tailer.currentFile()`）为界，**文件名比它小的（时间更早）都已读完，删除**，
遇到当前文件即停止。纯字符串比较，不解析时间、不涉及时区。删完后会再刷一次目录占用。

### 目录独占锁

打开目录时获取 `.spool.lock` 文件锁，同目录第二个进程/实例直接抛异常 fail fast，
从根上防止多进程操作同一目录。**一个目录只能被一个 Spool 实例打开。**

## 快速开始

```xml
<dependency>
    <groupId>com.dynamicbatch</groupId>
    <artifactId>dynamic-batch-spool</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

```java
// 1. 数据对象实现 Serializable（默认 JDK 原生序列化，所以需要实现java.io.Serializable）
public class DeviceDTO implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    private long id;
    private String name;
    // ...
}

// 2. 构造 Spool（不用传序列化器，默认 JDK 原生）
Spool<DeviceDTO> spool = Spool.builder(DeviceDTO.class, "/data/spool-device")
        .stagingCapacity(10000)           // 暂存队列容量（条数）
        .flushIntervalMs(5000)            // 刷盘间隔：断电最多丢 5 秒数据
        .rollCycleMillis(30_000)          // 30 秒滚一个文件（或 .rollCycle(RollCycles.FIVE_MINUTELY)）
        .offerTimeoutMs(100)              // 暂存队列满时最多等 100ms
        .cleanupIntervalMs(60_000)        // 每 60 秒清理一次已消费的旧文件
        .maxSizeBytes(1024L * 1024 * 1024) // 磁盘预算 1GB；默认 Long.MAX_VALUE=不限
        .dirSizeRefreshIntervalMs(5000)   // 每 5 秒刷新目录大小（默认 5000）
        .build();

// 3. 生产者（任意线程）
boolean ok = spool.append(deviceDTO);
if (!ok) {
    // 降级：暂存队列满，或目录占用已达 maxSizeBytes
}

// 可选：查看最近一次刷新的目录占用（字节）
long used = spool.getCurrentSizeBytes();

// 4. 消费者：lockTimeoutMs=1000 是获取锁的最长等待
//    有数据 → 返回对象；空队列 → 立即返回 null；锁超时 → 抛 TimeoutException
DeviceDTO dto = spool.poll(1000);
if (dto == null) {
    // 队列暂时没数据
}

// 5. 关闭（排空暂存 + 刷盘 + 释放目录锁）
spool.close();
```

## 自定义序列化器（换 JSON / Kryo 等）

默认 JDK 原生（对象需实现 `Serializable`）。想换格式，实现 `Serializer<T>` 接口即可——就两个方法：
`serialize(T)` 对象 → 字节（落盘用），`deserialize(byte[], Class<T>)` 字节 → 对象（还原用）。

```java
// Jackson 示例
Serializer<DeviceDTO> json = new Serializer<DeviceDTO>() {
    final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(DeviceDTO data) {
        try { return mapper.writeValueAsBytes(data); }
        catch (Exception e) { throw new RuntimeException(e); }
    }

    @Override
    public DeviceDTO deserialize(byte[] bytes, Class<DeviceDTO> type) {
        try { return mapper.readValue(bytes, type); }
        catch (Exception e) { throw new RuntimeException(e); }
    }
};

Spool<DeviceDTO> spool = Spool.builder(DeviceDTO.class, "/data/spool-device")
        .serializer(json)            // 覆盖默认序列化器
        .build();
```

## 定位与边界

**单进程内使用**。本模块给原本非并发安全的 Chronicle-Queue 封装了多线程并发安全：

- **多线程写**：生产者只入有界暂存队列（线程安全容器），后台单写线程串行落盘——任意线程可并发 `append`
- **多线程读**：共享 tailer 由一把锁串行化——任意线程可并发 `poll`，不重复不遗漏
- **多进程**：不保证并发安全，靠目录独占锁（`.spool.lock`）在打开时 fail fast 拦截第二个进程

> 目录锁是**协作式**的：只拦住"走正常流程尝试加锁"的第二个实例。如果其他程序不管锁、直接改目录里的文件，照样会改坏数据——**队列目录是内部文件，必须信任使用方不越权操作**。

## 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `type` / `dir` | 必填 | 数据类型 / 队列目录 |
| `serializer` | JDK 原生 | 序列化器，不传默认 JDK（对象需 Serializable），可用 `.serializer()` 换 JSON/Kryo 等 |
| `stagingCapacity` | 10000 | 暂存队列容量（条数），满则 append 阻塞等待 |
| `offerTimeoutMs` | 100 | 暂存队列满时 append 最多等多久，超时返回 false |
| `flushIntervalMs` | 5000 | 刷盘间隔。数据文件 + 读位置（offset）一并 sync，断电丢失窗口 / 重复消费窗口均 = 该值 |
| `rollCycle` / `rollCycleMillis` | FIVE_MINUTELY | 文件滚动周期，可传 `RollCycles` 枚举或毫秒数 |
| `cleanupIntervalMs` | 0（=滚动周期） | 清理已消费旧文件的间隔，滚动周期大时建议单独调小 |
| `maxSizeBytes` | Long.MAX_VALUE（不限） | 磁盘预算上限（字节）；目录占用 ≥ 上限时 append 返回 false |
| `dirSizeRefreshIntervalMs` | 5000 | 定时刷新目录大小的间隔；清理旧文件后也会立刻刷新 |

相关只读 API：`getCurrentSizeBytes()` 最近一次占用；需要时可手动 `refreshDiskUsage()`。

## 注意事项

### 宕机丢失风险（按丢失窗口从小到大）

| 资源 | 丢失窗口 | 后果 |
|------|---------|------|
| **暂存队列（内存）** | 写线程未取走的数据（≤ stagingCapacity 条） | **真丢**：append 返回 true ≠ 已持久化 |
| **未 sync 的 mmap 脏页** | 最后 flushIntervalMs 内的写入 | **真丢**（仅断电/OS 崩溃；kill -9 时 OS 仍在，page cache 通常能回写） |
| **读位置（offset）** | 最后 flushIntervalMs 内的已读记录 | **不丢**：重启后从断点续读，可能**重复消费**最近几条。offset 随数据一起定时 sync，重复窗口与数据丢失窗口一致 |
| **已 poll 未处理完** | poll 即交付 | **真丢**：读位置已推进，重启后跳过不补发。接受的设计取舍，处理失败需调用方自行重试 |

一句话：**内存丢数据、断电丢磁盘数据、offset 回退只产生重复消费**。

### 删除文件的延时风险

清理逻辑以 `tailer.currentFile()` 为界，且由后台线程**周期性**触发（默认 = 滚动周期）：

1. **消费停止 → 磁盘不释放**：tailer 不前进，旧文件永远不会被删，数据全部留在磁盘。这是设计意图（磁盘即容量），但需确保磁盘预算充足
2. **删除最长滞后一个清理周期**：即使数据已读完，也要等下一次定时任务触发才删
3. **tailer 当前所在文件不删**：最后一个被读的文件要等 tailer 进入下一个周期才删除，属预期行为
4. **Windows 删除失败**：文件被其他程序占用时删除失败，记日志、下次清理重试（Linux 无此问题）

### 其他

- **目录独占**：一个目录只允许一个 Spool 实例；不同类型的数据用不同目录（`Spool<DeviceDTO>` 与 `Spool<UserDTO>` 不能共用目录）
- **多进程**：不支持多进程读写同一目录（目录锁直接挡掉）
- **多线程并发 poll**：支持但**不保证单个消费者视角有序**；需要保序时用「单 reader 线程 + 线程池 worker」模式
- **poll 出去的数据**：Spool 不追踪处理结果，下游拒绝时必须自行重试同一条，不能丢弃
- **队列目录是内部文件**：`.cq4` 数据文件、`metadata.cq4t` 元数据，不要手动打开/修改/删除
- **磁盘预算单位是字节**：含未 cleanup 的历史文件与 Chronicle 预留块；`append == false` 时区分不了「队列满」与「超预算」，需结合日志 / `getCurrentSizeBytes()` 判断
