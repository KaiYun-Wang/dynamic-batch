# Dynamic Batch 使用手册

本手册说明如何接入与使用 Dynamic Batch。项目定位、架构与性能数据见 [项目 README](../README.md)。

**目录**

- [1. 适用场景](#1-适用场景)
- [2. 引入依赖](#2-引入依赖)
- [3. 基本用法](#3-基本用法)
- [4. 参数说明](#4-参数说明)
- [5. 有序性](#5-有序性)
- [6. 削峰与容量](#6-削峰与容量)
- [7. 限流](#7-限流)
- [8. 运行时调参](#8-运行时调参)
- [9. 监控与统计](#9-监控与统计)
- [10. 告警通知](#10-告警通知)
- [11. 运维端点](#11-运维端点)
- [12. 扩展点](#12-扩展点)
- [13. Spool 独立使用](#13-spool-独立使用)
- [14. 数据送达语义](#14-数据送达语义)
- [15. 调优建议](#15-调优建议)
- [16. 常见问题](#16-常见问题)

---

## 1. 适用场景

数据链路一句话：`submit` 先落磁盘缓冲，后台异步分发到分区，攒满一批调用你的回调写下游。

**适合**

- 高频低价值写入（明细流水、设备状态、埋点、账单），需要合并为批量接口调用。
- 存在瞬时流量尖峰，希望以本地磁盘缓冲削峰，而不为此引入消息队列。
- 业务键维度需要保序（同一订单/设备的变更按提交顺序生效）。
- 下游写入能力有限，需要匀速投递并可运行时调整速率。

**不适合**

- 需要跨进程/跨机器的消息分发与消费组语义（这是消息队列的职责，本框架不做网络传输）。
- 要求严格 once-only 送达（见 [14 数据送达语义](#14-数据送达语义)）。

---

## 2. 引入依赖

### 2.1 Spring Boot 接入

```xml
<repositories>
  <repository>
    <id>jitpack.io</id>
    <url>https://jitpack.io</url>
  </repository>
</repositories>

<dependency>
  <groupId>com.github.KaiYun-Wang.dynamic-batch</groupId>
  <artifactId>dynamic-batch-spring-boot-starter</artifactId>
  <version>1.0.0</version>
</dependency>
```

版本号即 git tag 名，须与仓库发布的 tag 完全一致（本仓库 tag 即 `1.0.0`，与 Maven 版本号同命名，不带前缀）。Starter 自动装配 `BatchProcessor`（随 Spring 容器关闭优雅停机）。该装配带 `@ConditionalOnMissingBean`：需自行接管生命周期时，直接定义一个 `BatchProcessor` Bean 即可。

### 2.2 非 Spring 环境

```xml
<dependency>
  <groupId>com.github.KaiYun-Wang.dynamic-batch</groupId>
  <artifactId>dynamic-batch-core</artifactId>
  <version>1.0.0</version>
</dependency>
```

```java
BatchProcessor processor = new BatchProcessor();
processor.registerGroup("device_insert", group);
// ... 业务运行
processor.shutdown();   // 进程退出前显式调用
```

此方式下告警投递、容量巡检与 HTTP 端点不可用（属 Spring 集成层能力），其余功能完整。

---

## 3. 基本用法

接入只需两步：注册业务组、提交数据。

### 3.1 注册业务组

`flushCallback`（攒满一批后怎么写下游）与 `spoolDir`（磁盘缓冲目录）为必填项，**每个组的目录必须独占**，不可跨组共用。

```java
@Configuration
public class BatchConfig {

    private final BatchProcessor batchProcessor;

    public BatchConfig(BatchProcessor batchProcessor) {
        this.batchProcessor = batchProcessor;
    }

    @PostConstruct
    void registerGroups() {
        batchProcessor.registerGroup("device_insert",
                BatchWorkerGroup.builder(DeviceDTO.class,
                                SpoolConfigPOJO.builder("/data/batch/device_insert").build(),
                                this::flushDevice)
                        .partitionCount(4)     // 分区数：并行度
                        .batchSize(100)        // 攒满 100 条触发回调
                        .maxWaitMs(1000)       // 未满批最长等 1s
                        .rateLimit(5000)       // 可选：组级限速（条/秒）
                        .failureHandler(this::onFlushFailed)  // 可选：失败兜底
                        .build());
    }

    private void flushDevice(List<DeviceDTO> batch) {
        deviceMapper.insertBatchSomeColumn(batch);   // 事务与幂等由业务保证
    }

    private void onFlushFailed(List<DeviceDTO> batch) {
        // 落补偿表 / 记录死信；框架不做自动重试
    }
}
```

### 3.2 提交数据

```java
// groupKey：业务组名；routingKey：业务键（同键有序）
boolean accepted = batchProcessor.submit("device_insert", device.getDeviceId(), device);
if (!accepted) {
    // submit 不抛异常，被拒时自行决策：重试、降级直写或丢弃计数
}
```

### 3.3 验证接入

```bash
curl http://localhost:8080/actuator/dynamicbatch/device_insert
```

确认 `running` 为 `true`、`stats.submitTotal` 随提交递增。可运行示例见 `example-boot27`、`example-boot35`、`example-extension`。

---

## 4. 参数说明

所有配置在注册阶段即完成校验，非法参数直接抛异常，不会带病启动。

### 4.1 组参数（`BatchWorkerGroup.Builder`）

| 参数 | 默认 | 说明 | 可热更 |
|------|------|------|--------|
| `partitionCount` | 1 | 分区数，即组内并行度 | 走[三步流程](#83-变更分区数) |
| `queueCapacity` | 1024 | 每分区内存队列容量 | 否 |
| `batchSize` | 50 | 攒满几条触发回调，须 ≤ `queueCapacity` | 是 |
| `maxWaitMs` | 1000 | 未满批最长等待，0 表示每条立即刷 | 是 |
| `rateLimit` | 不限流 | 组级投递限速（条/秒），须 > 0 | 是 |
| `shutdownTimeoutMs` | 5000 | 优雅关闭总预算（毫秒） | 否 |
| `failureHandler` | 无 | 回调抛异常时的兜底入口 | 否 |
| `statsConfig` | 默认 | 统计配置，见 4.3 | 否 |

### 4.2 Spool 参数（`SpoolConfigPOJO.Builder`）

`spoolDir` 为构造参数且必填；其余不设置即用默认值。

| 参数 | 默认 | 说明 |
|------|------|------|
| `spoolDir` | 必填 | 队列目录，每组独占；被其他进程占用时启动失败 |
| `maxSizeBytes` | 不限制 | 磁盘预算（字节），达界后 `submit` 被拒；按「可容忍积压时长 × 峰值字节速率」估算 |
| `flushIntervalMs` | 5000 | 落盘 sync 间隔，越小断电丢失越少、吞吐略降 |
| `rollCycleMillis` | 5 分钟 | 数据文件滚动周期（毫秒），最小 1000 |
| `cleanupIntervalMs` | 跟随滚动周期 | 已消费文件的清理间隔；滚动周期设大时应单独调小，否则空间释放滞后 |
| `offerTimeoutMs` | 100 | 内存暂存满时 `submit` 的最长等待，超时拒绝；0 表示立即拒绝 |
| `stagingCapacity` | 10000 | 写路径内存暂存容量（条数），进程崩溃时其中数据丢失 |
| `dirSizeRefreshIntervalMs` | 5000 | 磁盘占用刷新间隔，用于预算判定 |
| `serializer` | JDK 序列化 | 载荷编解码器；默认 JDK 要求载荷实现 `Serializable` |

### 4.3 统计参数（`StatsConfigPOJO.Builder`）

| 参数 | 默认 | 说明 |
|------|------|------|
| `enabled` | true | 关闭后无统计快照、`StatsListener` 不触发 |
| `collectIntervalMillis` | 5000 | 采集周期（毫秒） |
| `percentiles` | 空 | 分位数（0,1]，如 `0.5, 0.99, 0.999`；配置后才产出 tp 值 |
| `rtBuckets` | 1-2-5 序列 | RT 分桶上界（毫秒），一般无需调整 |

---

## 5. 有序性

以业务键作为 `routingKey` 提交，同一 key 恒定进入同一分区，分区内按提交顺序单线程消费，即**业务键维度严格有序**；分区之间并行、互不保证顺序。

使用注意：

1. 不同 key 可能哈希到同一分区，彼此排队属预期；若某 key 流量显著偏高（热点），会拖慢同分区其他 key，可细化 key 粒度（如「用户 ID + 日期」）或扩分区。
2. 变更分区数会重建分区集合，**变更点前后不保证同一 key 有序**，请在业务低峰期操作。
3. 顺序性的代价是队头阻塞：某分区消费停滞时整组投递暂停（宁可暂停也不破坏有序）。因此 `flushCallback` 内的下游调用**必须设置超时**，并配合监控各分区队列水位。

---

## 6. 削峰与容量

`submit` 只写磁盘缓冲即返回，耗时与下游完全解耦；下游按自身能力匀速消化，瞬时尖峰由磁盘吸收。

**两处拒绝边界**（任一触发即 `submit` 返回 `false`）：

| 边界 | 触发条件 | 表现 |
|------|----------|------|
| 内存暂存队列 | 提交速度持续高于落盘速度 | 阻塞至多 `offerTimeoutMs` 后拒绝 |
| 磁盘预算 | 目录占用达到 `maxSizeBytes` | 立即拒绝，直至消费推进释放空间 |

调用方必须处理 `false` 分支，否则尖峰期间会静默丢数据。

**水位治理**：配置 `spool_capacity` 告警项（见 [10 告警通知](#10-告警通知)），占用达预算 `threshold`（百分比）时周期推送；滚动周期大时显式调小 `cleanupIntervalMs`。

**断电与重启**：数据写入采用内存映射文件，持久化有两条路径——OS 会自动将脏页刷回磁盘（时机由内核决定，随机且窗口较大），框架另按 `flushIntervalMs` 定时主动 sync 收窄窗口。因此只要系统没有整机断电/宕机，**已写入映射的数据不会因进程崩溃而丢失**；读位置同样持久化，重启后自动续读。

| 故障 | 影响 |
|------|------|
| 进程崩溃 | 仅丢失仍在内存暂存队列、尚未写入映射的数据（≤ `stagingCapacity` 条）；已写入映射的数据由 OS 兜底落盘，不丢 |
| 整机断电 / 宕机 | 上述之外，OS 页缓存中尚未刷盘的脏页可能丢失（OS 刷盘时机随机，定时 sync 之外的部分不保证）；同时读位置与数据文件的落盘进度不保证同步，**读位置可能回退，导致已消费数据被重复读取，幂等性需业务侧自行保证** |

对断电敏感的业务可调小 `flushIntervalMs` 主动收窄窗口，以写吞吐为代价。

---

## 7. 限流

配置 `rateLimit` 后，分发线程按该速率（条/秒）向下游投递，空闲期不积累突发。典型用法：下游单库写入能力约 5000 行/秒，则设 `rateLimit(4000)` 留出余量，上游尖峰由 Spool 吸收。

- 限速是**组级总量**，扩分区不会提升总速率，只改变分摊方式；
- 限速值可热更（见下节），下一投递即生效。

---

## 8. 运行时调参

### 8.1 参数热更

```java
// null 表示该字段不改；下一批 / 下一投递起生效
batchProcessor.resizeGroupConfig("device_insert", 200, 500L, 8000);
```

### 8.2 暂停与恢复

```java
batchProcessor.pauseDispatcher("device_insert");   // 异步意图，立即返回
// 暂停期间 submit 照常落盘，恢复后自动消化积压
batchProcessor.resumeDispatcher("device_insert");
```

调度器相位：`RUNNING` / `PAUSE_PENDING` / `PAUSED` / `RUN_PENDING`，经快照的 `dispatcherPhase` 字段查询。

### 8.3 变更分区数

按「暂停 → 等待生效 → 变更 → 恢复」四步操作：

```java
batchProcessor.pauseDispatcher("device_insert");
while (!"PAUSED".equals(batchProcessor.getGroupSnapshot("device_insert").getDispatcherPhase())) {
    Thread.sleep(50);
}
batchProcessor.resizePartitions("device_insert", 8, 30_000L);   // 预算内未排空抛 TimeoutException，可直接重试
batchProcessor.resumeDispatcher("device_insert");
```

变更语义见 [5 有序性](#5-有序性) 注意点 2。

---

## 9. 监控与统计

### 9.1 查询快照

Java API：`getGroupSnapshot(key)` / `listGroupSnapshots()`；HTTP：`GET /actuator/dynamicbatch/{key}`。主要字段：

| 字段 | 含义 |
|------|------|
| `queueSizes` | 各分区内存队列水位，下标即分区号 |
| `stagingSize` | 暂存积压条数（落盘跟不上的直接信号） |
| `spoolUsage` | 磁盘占用拆分：`totalBytes` = `consumedBytes`（已读未删）+ `pendingBytes`（未读） |
| `dispatcherPhase` | 调度器相位 |
| `stats` | 统计快照：`submitQps` / `callbackQps` / `submitTotal` / `callbackTotal` / RT 分桶 `bucketCounts` / 分位值 `tp` |

`stats.tp` 需配置 `percentiles` 才有值；各字段为瞬时读取，`tp` 为本采集周期近似值，不可跨周期平均。

### 9.2 推送自建监控

```java
StatsListeners.register(snapshot -> {
    // 每 5s（collectIntervalMillis）一次，按 groupKey 区分数据来源
    log.info("group={} qps={} tp={}", snapshot.getGroupKey(),
            snapshot.getSubmitQps(), snapshot.getTp());
});
```

钩子在采集线程上同步执行，应快速返回，重 IO 请自行异步；多个钩子互相隔离，单个异常不影响采集。

---

## 10. 告警通知

配置前缀 `dynamic-batch.notify`；`platforms` 为空时告警整体关闭。通知异步投递，不占业务线程。

```yaml
dynamic-batch:
  notify:
    platforms:
      - platform: ding            # 钉钉
        webhook: "https://oapi.dingtalk.com/robot/send?access_token=xxx"
        secret: ""                # 加签密钥，可空
        receivers: "all"          # @所有人，或逗号分隔的手机号
        timeout: 3000
      - platform: wechat          # 企业微信
        webhook: "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx"
        timeout: 3000
      - platform: email           # 邮件，需自行引入 spring-boot-starter-mail
        host: "smtp.example.com"
        port: 465                 # 465 走 SSL，其余端口自动启用 STARTTLS
        username: "alert@example.com"
        password: "SMTP 授权码"
        title: "攒批通知"
        receivers: "ops@example.com,dba@example.com"
    notify-items:                 # 显式登记且 enabled=true 才投递
      - type: offer_failed        # 入队失败（组未启动/类型不匹配/被拒）
        enabled: true
        silence-period: 300       # 静默期（秒）
      - type: flush_failed        # 回调执行失败
        enabled: true
        silence-period: 300
      - type: spool_capacity      # 磁盘水位巡检
        enabled: true
        threshold: 70             # 预算占用百分比
        interval-seconds: 60      # 巡检周期
        silence-period: 600
```

内置渠道为 `ding`、`wechat`、`email`；其余平台（如飞书）通过 `Notifier` SPI 扩展，`example-extension` 提供完整实现。

---

## 11. 运维端点

```yaml
management:
  endpoints:
    web:
      exposure:
        include: dynamicbatch
```

前置条件：应用为 servlet web 应用且引入 actuator；`dynamic-batch.actuator.enabled: false` 可显式关闭。运维能力的本体是 `BatchProcessor` 的 Java API，非 Web 进程直接调用即可。

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/actuator/dynamicbatch` | 全部组快照 |
| GET | `/actuator/dynamicbatch/{key}` | 单组快照，组不存在返回 404 |
| POST | `/actuator/dynamicbatch/{key}` | 热更 `batchSize` / `maxWaitMs` / `rateLimitPerSecond`（缺参不改） |
| POST | `/actuator/dynamicbatch/{key}/pause` | 暂停调度器 |
| POST | `/actuator/dynamicbatch/{key}/resume` | 恢复调度器 |
| POST | `/actuator/dynamicbatch/{key}/resize` | 变更分区数：`newSize` 必填，`timeoutMs` 默认 30000，须先暂停 |

错误返回码：400 参数非法 / 组不存在，409 状态不允许（如未暂停就 resize），504 排空超时（分区数未变，可重试），500 其他异常。

端点为**单实例视角**，不做集群聚合；具备写能力，生产环境务必置于内网并接入鉴权。

---

## 12. 扩展点

| 扩展接口 | 用途 | 说明 |
|----------|------|------|
| `Serializer<T>` | 自定义落盘编解码 | 默认 JDK；可接 Jackson / Kryo / Protobuf |
| `Notifier` | 新增告警渠道 | 实现 `platform()` / `send()`，飞书示例见 `example-extension` |
| `NoticeTemplate` | 自定义告警文案 | 按 `NotifyTypeEnum` 绑定 |
| `JsonParser` | 替换 JSON 引擎 | 按依赖探测：Jackson → FastJson → Hutool → 内置零依赖兜底 |
| `StatsListener` | 统计出口 | 对接 Prometheus / 自建监控 |
| `failureHandler` | 批次失败兜底 | 组级构造参数 |

扩展实现方式参考 `example-extension` 模块。

---

## 13. Spool 独立使用

Spool 是独立的磁盘缓冲队列模块（写侧有界暂存 + 串行落盘，读侧读位置持久化、目录独占锁），**不依赖批处理核心**，获取方式任选其一：

1. **单独引入 artifact**（推荐）：

   ```xml
   <dependency>
     <groupId>com.github.KaiYun-Wang.dynamic-batch</groupId>
     <artifactId>dynamic-batch-spool</artifactId>
     <version>1.0.0</version>
   </dependency>
   ```

2. **已引入 starter**：spool 是其传递依赖，classpath 中直接使用 `Spool` 类即可；
3. **拷贝源码**：`dynamic-batch-spool` 模块仅依赖 `chronicle-queue` 与 `slf4j-api`，可将该模块源码直接复制进自己的工程。

```java
// poll 的锁等待超时抛受检 TimeoutException
void consume() throws TimeoutException {
    try (Spool<OrderMsg> spool = Spool.builder(OrderMsg.class, "/data/spool/order")
            .maxSizeBytes(4L * 1024 * 1024 * 1024)    // 磁盘预算 4GB
            .flushIntervalMs(1000)
            .stagingCapacity(20000)
            .build()) {

        spool.append(msg);                            // 生产侧：入暂存队列即返回

        OrderMsg data = spool.poll(100);              // 消费侧：队列为空返回 null
        if (data == null) {
            spool.awaitData(200);                     // 写侧落盘信号唤醒，避免空轮询
        }
    }
}
```

主要 API：`append` / `poll` / `awaitData` / `stagingSize` / `getCurrentSizeBytes` / `diskUsage` / `close`。

**与 starter 并存**：引入 starter 后自建 Spool 实例没有任何冲突（starter 只装配 `BatchProcessor` 门面，不创建 Spool、不注册组）；区别仅在于自建实例不进入 Actuator 快照与容量巡检告警的管理范围。

---

## 14. 数据送达语义

框架提供「磁盘持久化 + 尽力送达」，不是严格 once-only。持久化由 OS 自动刷盘与框架定时 sync 共同完成：进程崩溃不影响已写入映射的数据，**整机断电 / 宕机才是真正的丢失与重复边界**。

| 保证 | 说明 |
|------|------|
| 已写入映射的数据，进程崩溃不丢 | 数据由 OS 页缓存持有并自动刷盘，进程消亡不影响 |
| 正常重启不重复消费 | 读位置持久化，正常关闭与重启不会重复读取 |
| 分区内有序 | 见 [5 有序性](#5-有序性) |

| 不保证 | 说明 |
|--------|------|
| 暂存队列数据不丢 | 已提交但仍在内存暂存队列、尚未写入映射的数据，进程崩溃即丢失（≤ `stagingCapacity` 条） |
| 整机断电 / 宕机零丢失 | 页缓存中未刷盘的脏页可能丢失；OS 刷盘时机随机，定时 sync 只是主动收窄窗口 |
| 整机断电 / 宕机不重复 | 读位置与数据落盘进度可能不同步，读位置回退会导致重复消费，**幂等性需业务侧在回调中自行保证** |
| 失败批次自动恢复 | 依赖业务 `failureHandler`（框架不做自动重试，重试策略与业务幂等语义强耦合） |

已知风险点：慢回调会冻结整组投递（见 [5 有序性](#5-有序性) 注意点 3）；队列所在盘空间被耗尽时落盘受阻，应对磁盘单独做容量告警；Windows 上 `maxWaitMs` 低于 ~15.6ms 会被定时器粒度放大，建议 ≥ 100。

---

## 15. 调优建议

| 目标 | 手段 |
|------|------|
| 提高吞吐 | 增大 `batchSize`、`partitionCount`，放宽 `maxWaitMs` |
| 降低延迟 | 减小 `maxWaitMs` 与 `batchSize`（`maxWaitMs=0` 每条即刷，下游调用次数成倍增加） |
| 控制内存 | 下调 `queueCapacity` 与 `stagingCapacity`，二者共同决定积压上限 |
| 保护下游 | `rateLimit` 设为下游能力的 70%~80%，配合 `spool_capacity` 告警 |
| 抗尖峰 | `maxSizeBytes` 留足余量 + 缩短 `cleanupIntervalMs` 保证空间按节奏释放 |
| 减少断电丢失 | 调小 `flushIntervalMs` |

参数联动约束：`batchSize ≤ queueCapacity`；组级内存队列上限 = 分区数 × `queueCapacity`。

---

## 16. 常见问题

**Q：`submit` 返回 false 但没有异常？**
设计契约：入口不向业务线程抛异常。检查组是否已注册启动、数据类型与注册 `type` 是否一致、磁盘预算/暂存是否已满；开启 `offer_failed` 告警可直接收到被拒原因。

**Q：某组完全不消费，快照 `dispatcherPhase` 是 `PAUSED`？**
此前执行过暂停且未恢复，调用 `resumeDispatcher` 即可，积压自动消化。

**Q：`resize` 返回 409？**
调度器未处于已暂停状态。`pauseDispatcher` 是异步意图，需等快照显示 `PAUSED` 再变更。

**Q：两个组能否共用一个 `spoolDir`？**
不能。目录被单组独占加锁，共用会在启动期直接失败；多实例部署同理，每组目录须互不相同。

**Q：重启后会重复消费吗？**
正常关闭与重启不会重复读取已消费数据（读位置已持久化）。整机断电 / 宕机后读位置可能回退，存在重复消费的可能，业务侧应按业务键做幂等（见 [14](#14-数据送达语义)）。

---

## 相关文档

- [项目 README](../README.md)：定位、特性、架构图、性能基准
- [SinglePartitionPerf 基准报告](benchmark/SinglePartitionPerf‑msg‑size‑bench‑summary.md)
- 交互式视图（克隆源码后以浏览器打开）：`docx/architecture/architecture.html`、`docx/benchmark/SinglePartitionPerf‑msg‑size‑bench‑summary.html`
