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
                .hotUpdateType(BatchWorkerHotUpdateType.ENDPOINT)  // 声明热更新通道，见 3.3
                .build()
        );
    }

    private void flushInsert(List<DeviceDTO> batch) {
        // 自行保证事务、幂等；框架不做自动重试
    }
}
```

**注意：** `flushCallback` 抛异常时自动发批次失败告警（见 §6）；`failureHandler` 可选，未配置时告警消息会标记数据丢失风险。

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
batchProcessor.refresh("device_dto_insert", config, BatchWorkerHotUpdateType.ENDPOINT);
// null 字段 = 不更新
```

**通道隔离规则：**

- 热更新必须显式声明通道 `type`，且须与 Worker 构建时声明的 `hotUpdateType` **完全一致**，否则抛 `IllegalArgumentException`；
- Worker 未声明通道（构建时没调 `.hotUpdateType(...)`）时，**任何通道的刷新都会被拒绝**（即该 Worker 不支持热更新）；
- 通道枚举：`ENDPOINT`（Actuator 等本进程运维入口）、`CONFIG_CENTER`（配置中心推送，预留）。

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
- flush 失败 → `failureHandler`，**不自动重试**，同时自动发**批次失败告警**（见 §6）
- 容器关闭 → `BatchProcessor.shutdown()` 尽量刷剩余数据（starter 已配 `destroyMethod`）

---

## 6. 通知与告警（可选）

配置 `dynamic-batch.notify.platforms` 后，三类消息**触发即发**（异步，不阻塞业务线程）：

| 类型 | 触发时机 | 消息内容 |
|------|----------|----------|
| `change` 变更通知 | Worker `refresh` 成功且 diff 非空 | 变更字段与新旧值 |
| `offer_failed` 入队失败 | `submit` 被拒绝：已关闭 / 类型不匹配 / 队列满超时 / 中断 | 原因、入队超时、当前队列水位 |
| `flush_failed` 批次失败 | `flush` 回调抛异常（failureHandler 处理完之后） | 失败条数、异常、数据丢失风险 |

> 当前无任何配置项（阈值、静默限流、类型级路由均在规划中），事件发生即广播到全部平台；
> 未配置 `platforms` 时通知整体关闭，纯 core（无 spring）使用无任何通知行为。

**平台配置示例（ding / wechat / email）：**

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
      - platform: email        # 需引入 spring-boot-starter-mail
        host: "smtp.qq.com"   # SMTP 服务器
        port: 465              # SMTP 端口（465 走 SSL）
        username: "xxx@qq.com"
        password: "授权码"
        title: "dynamic-batch" # 邮件标题，默认“攒批通知”
        receivers: "ops@corp.com"
        timeout: 3000
```

代码里也可调 `NotifyService` 手动发（见 example `NotifyTestController`）；
失败场景自测见 example `NotifyFailureTestController`（队列满 / 类型不匹配 / 已关闭 / 兜底接管 / 数据丢失 5 个端点）。

---

## 7. Actuator 运维端点（可选）

**场景：** 单体 / 少实例 / 调试，不想接配置中心时手动热更、查状态。

> 热更前提：Worker 构建时声明了 `hotUpdateType = ENDPOINT`（见 §3.3），否则 POST 返回 `refresh failed: hotUpdateType mismatch`。

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
- [ ] 告警配置化：阈值、静默限流、检测型告警（队列积压）
- [ ] 通知路由：按通知类型指定平台 / 接收人

当前：**参数写 register 或 Actuator 热更；callback 只能代码注册；通知触发即发（无配置项）。**

---

## 11. API 速查

```text
// BatchProcessor
register(key, worker)           // 注册并启动
submit(key, data)               // 入队
refresh(key, configPOJO, type)   // 热更（type 须与 worker 声明一致）+ 变更通知
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

- `BatchProcessorConfiguration` — 注册 `demo_item_insert`、`demo_item_update`（声明了 ENDPOINT 热更新通道）
- `RefreshTestController` — 业务 Controller 调 refresh（对比 Actuator）
- `NotifyTestController` — 通知自测（手动发 ding/wechat/email）
- `NotifyFailureTestController` — 失败告警自测（offer-full / offer-type-mismatch / offer-stopped / flush-error / flush-error-loss）
- `BatchProcessorDemoTest` — submit 演示

本地跑：`mvn install -DskipTests` → `example-boot27` 启动 → curl Actuator。
