package com.dynamicbatch.example.controller;

import com.dynamicbatch.spring.notify.NotifyService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 通知自测入口。
 *
 * <p>填好 application.yml 中的 dynamic-batch.notify.platforms（钉钉 webhook/secret）后：
 * <pre>
 * curl -X POST "http://localhost:8080/notify/ding?content=你好，钉钉"
 * </pre>
 * 向钉钉发一条测试消息，发送结果见应用日志。
 */
@RestController
@RequestMapping("/notify")
public class NotifyTestController {

    private final NotifyService notifyService;

    public NotifyTestController(NotifyService notifyService) {
        this.notifyService = notifyService;
    }

    @PostMapping("/ding")
    public String sendDing(@RequestParam(defaultValue = "Dynamic-Batch 测试消息\n\n- 通知配置打通成功") String content) {
        boolean ok = notifyService.send("ding", content);
        return ok ? "已交给钉钉渠道发送（结果见日志）" : "发送失败：未配置 ding 平台或渠道未注册（见日志）";
    }
}
