# dynamic-batch 使用指南（速查）

> 开发备忘：核心功能与用法一览。

---

## 1. 是什么

进程内攒批框架：业务线程 `submit` 入队，后台消费线程攒批后调 `flush` 回调写库/写下游。

**模块：**

| 模块 | 作用 |
|------|------|
| `dynamic-batch-core` | 引擎：`BatchProcessor`、`BatchWorker` |
| `dynamic-batch-spring` | 自动配置、Actuator、通知 |
| `dynamic-batch-spring-boot-starter` | 引入即用 |
| `example-boot27` | 示例 |

---

## 2. 引入依赖

**最简（只要攒批）：**

```xml
<dependency>
  <groupId>com.dynamicbatch</groupId>
  <artifactId>dynamic-batch-spring-boot-starter</artifactId>
</dependency>
```

**可选：Actuator 运维端点（热更/查询）**

```xml
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
```

```yaml
management:
  endpoints:
    web:
      exposure:
        include: dynamicbatch
```

---

## 3. 核心用法（必做）

### 3.1 注册 Worker

`flushCallback` **必须在代码里写**，不能放 YAML。

```java
@Configuration
public class BatchProcessorConfiguration {

    public static final String DEVICE_INSERT = "device_dto_insert";

    private final BatchProcessor batchProcessor;

    public BatchProcessorConfiguration(BatchProcessor batchProcessor) {
        this.batchProcessor = batchProcessor;
    }

    @PostConstruct
    void register() {
        batchProcessor.register(DEVICE_INSERT,
            BatchWorker.builder(DeviceDTO.class, this::flushInsert)
                .queueCapacity(100)
                .batchSize(50)
                .maxWaitMs(1000)
                .offerTimeoutMs(100)
                .consumers(1)
                .failureHandler(failed -> log.error("flush failed, size={}", failed.size()))
                .build()
        );
    }

    private void flushInsert(List<DeviceDTO> batch) {
        // 自行保证事务、幂等；框架不做自动重试
    }
}
```

### 3.2 提交数据

```text
boolean ok = batchProcessor.submit("device_dto_insert", deviceDTO);
// false：key 不存在 / 队列满超时 / 类型不匹配 / worker 已关闭
```

### 3.3 热更新（运行时改参数）

```text
BatchWorkerConfigPOJO config = new BatchWorkerConfigPOJO();
config.setBatchSize(100);   // 只 set 要改的字段
config.setQueueCapacity(200);
batchProcessor.refresh("device_dto_insert", config);
// null 字段 = 不更新
```

可热更字段：`queueCapacity`、`batchSize`、`maxWaitMs`、`offerTimeoutMs`、`consumers`。

---

## 4. Worker key 命名规范

**仅允许：** 字母、数字、`-`、`_`（注册时校验，非法抛 `IllegalArgumentException`）。

```text
推荐：device_dto_insert、demo_item_update
避免：DemoItem:insert（冒号 URL 不友好）
```

key 在**本 JVM 内唯一**；集群里每台实例各自一份同名 Worker。

---

## 5. Worker 参数说明

| 参数 | 含义 | 默认（见 `BatchWorkerConstant`） |
|------|------|----------------------------------|
| `queueCapacity` | 队列容量 | 1024 |
| `batchSize` | 攒够几条 flush | 50 |
| `maxWaitMs` | 未满批最长等待 ms | 1000 |
| `offerTimeoutMs` | 入队阻塞超时 ms | 100 |
| `consumers` | 消费线程数（同队列竞争） | 1 |

**设计约定：**

- 事务、幂等、重试 → **flush 回调自己管**
- flush 失败 → `failureHandler`，**不自动重试**
- 容器关闭 → `BatchProcessor.shutdown()` 尽量刷剩余数据（starter 已配 `destroyMethod`）

---

## 6. 变更通知（可选）

配置 `dynamic-batch.notify.platforms`，Worker **refresh 成功且 diff 非空**时异步发钉钉/企微。

```yaml
dynamic-batch:
  notify:
    platforms:
      - platform: ding
        webhook: "https://oapi.dingtalk.com/robot/send?access_token=xxx"
        secret: ""          # 加签密钥，无则留空
        receivers: "all"
        timeout: 3000
      - platform: wechat
        webhook: "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx"
        timeout: 3000
```

代码里也可调 `NotifyService` 手动发（见 example `NotifyTestController`）。

---

## 7. Actuator 运维端点（可选）

**场景：** 单体 / 少实例 / 调试，不想接配置中心时手动热更、查状态。

| 方法 | 路径 | 作用 |
|------|------|------|
| GET | `/actuator/dynamicbatch` | 列出全部 Worker |
| GET | `/actuator/dynamicbatch/{key}` | 查单个 Worker |
| POST | `/actuator/dynamicbatch/{key}` | 热更新（JSON 扁平字段） |

POST body 示例：

```json
{ "batchSize": 10, "queueCapacity": 200 }
```

PowerShell：`curl.exe ... -d '{"batchSize":10}'`（单引号包 JSON）。

关闭端点：`dynamic-batch.actuator.enabled: false`

---

## 8. 部署场景怎么选

| 场景 | 推荐 |
|------|------|
| 单实例 / 本地调试 | 代码 register + 可选 Actuator 热更 |
| 多实例集群，改一次要全生效 | **配置中心**（待做）：每台 listener → 本地 `refresh()` |
| 多实例，偶尔改参数 | 运维逐台调 Actuator，或 rolling restart |
| 业务 submit | 走网关负载均衡即可，每台各自攒批 |

**记住：** Actuator / `refresh()` 都是**单进程视角**；网关默认只打一台，不会自动广播全集群。

---

## 9. 自动装配了什么

starter 启动后自动提供：

- `BatchProcessor` Bean（`destroyMethod=shutdown`）
- `NotifyProperties` + `NotifyService`（配了 notify 时）
- `BatchEndpoint`（classpath 有 Actuator 且 exposure 包含 `dynamicbatch` 时）

**不会自动：** 注册 Worker（必须业务代码 `register`）。

---

## 10. 尚未实现（规划）

- [ ] `BatchProperties` + YAML 绑定 Worker 参数
- [ ] 配置中心 starter（Nacos / Spring Cloud `EnvironmentChangeEvent` → refresh）
- [ ] 集群聚合查询 / 统一管控台

当前：**参数写 register 或 Actuator 热更；callback 只能代码注册。**

---

## 11. API 速查

```text
// BatchProcessor
register(key, worker)           // 注册并启动
submit(key, data)               // 入队
refresh(key, configPOJO)        // 热更 + 可选通知
listWorkerKeys()                // 本机 key 列表
getWorkerInfo(key)              // 运行时快照 → BatchWorkerInfoVO
listWorkerInfos()               // 全部快照
shutdown()                      // 关闭所有 Worker

// 静态校验
BatchProcessor.validateWorkerKey(key)
```

---

## 12. 示例项目

`example-boot27`：

- `BatchProcessorConfiguration` — 注册 `demo_item_insert`、`demo_item_update`
- `RefreshTestController` — 业务 Controller 调 refresh（对比 Actuator）
- `NotifyTestController` — 通知自测
- `BatchProcessorDemoTest` — submit 演示

本地跑：`mvn install -DskipTests` → `example-boot27` 启动 → curl Actuator。
