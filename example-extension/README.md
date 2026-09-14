# example-extension

演示如何扩展 dynamic-batch：飞书通知渠道、告警文案模板、JSON 引擎、统计快照钩子。

```bash
# 1. 填写 application.yml 中的飞书 webhook / secret
# 2. 启动
mvn -pl example-extension -am spring-boot:run
```

默认端口 `8088`。

## 飞书通知渠道（`Notifier` SPI）

实现：`FeishuNotifier`，经 `META-INF/services/...Notifier` 注册，yml `platform: feishu`。

```bash
curl -X POST "http://localhost:8088/extension/feishu?content=hello"
```

## 告警文案模板（`NoticeTemplate`）

实现：`FeishuOfferFailedNoticeTemplate` / `FeishuFlushFailedNoticeTemplate`，启动时 `NotifyManager.registerTemplate` 覆盖内置模板。

```bash
curl "http://localhost:8088/extension/notice/preview"
curl -X POST "http://localhost:8088/extension/notice/offer-failed"
curl -X POST "http://localhost:8088/extension/notice/flush-failed"
```

## JSON 引擎（`JsonParser` SPI）

实现：`DemoGsonJsonParser`，经 `META-INF/services/...JsonParser` 注册；`JsonUtil` 使用第一个 `supports()` 为 true 的实现。

```bash
curl "http://localhost:8088/extension/json"
```

## 统计快照钩子（`StatsListener`）

启动后自动采集。可多次 `StatsListeners.register`，每轮快照会依次回调全部钩子。控制台约每 5 秒出现：

```text
[stats-hook-A/json] ...
[stats-hook-B/summary] ...
[stats-hook-C/lambda] ...
```
