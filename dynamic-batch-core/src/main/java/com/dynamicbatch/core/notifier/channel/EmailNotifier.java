package com.dynamicbatch.core.notifier.channel;

import com.dynamicbatch.common.enums.NotifyPlatformEnum;
import com.dynamicbatch.common.pojo.NotifyPlatformPOJO;
import com.dynamicbatch.common.util.MarkdownUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

/**
 * 邮件通知渠道。
 *
 * <p>读取 {@link NotifyPlatformPOJO} 中的 SMTP 参数（host/port/username/password/title）
 * 直接发送邮件，不依赖 Spring Boot MailSenderAutoConfiguration。
 * 通过 SPI 加载，{@link #supports()} 探测 classpath 中是否存在
 * {@code javax.mail.Session}；不存在时静默跳过，不影响其他渠道。
 * 设计参考 dromara dynamic-tp 的 extension-notify-email 模块。
 */
public class EmailNotifier extends AbstractNotifier {

   private static final Logger log = LoggerFactory.getLogger(EmailNotifier.class);

    @Override
    public String platform() {
        return NotifyPlatformEnum.EMAIL.name().toLowerCase();
    }

    @Override
    public boolean supports() {
        try {
            Class.forName("javax.mail.Session");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    @Override
    protected void doSend(NotifyPlatformPOJO platform, String content) throws Exception {
        String host = platform.getHost();
        if (host == null || host.isEmpty()) {
            log.warn("email notifier skipped, host not configured for platform={}", platform.getPlatformId());
            return;
        }

        Session session = createSession(platform);
        MimeMessage message = buildMessage(session, platform, content);
        sendMessage(session, message, platform);
    }

    private Session createSession(NotifyPlatformPOJO platform) {
        Properties props = new Properties();
        props.put("mail.smtp.host", platform.getHost());
        props.put("mail.smtp.port", String.valueOf(platform.getPort() != null ? platform.getPort() : 465));
        props.put("mail.smtp.auth", "true");

        int timeout = platform.getTimeout() != null ? platform.getTimeout() : 10000;
        props.put("mail.smtp.connectiontimeout", String.valueOf(timeout));
        props.put("mail.smtp.timeout", String.valueOf(timeout));

        int port = platform.getPort() != null ? platform.getPort() : 465;
        if (port == 465) {
            props.put("mail.smtp.socketFactory.port", String.valueOf(port));
            props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
            props.put("mail.smtp.socketFactory.fallback", "false");
        } else {
            props.put("mail.smtp.starttls.enable", "true");
        }

        return Session.getInstance(props, null);
    }

    private MimeMessage buildMessage(Session session, NotifyPlatformPOJO platform, String content)
            throws MessagingException, UnsupportedEncodingException {
        MimeMessage message = new MimeMessage(session);
        message.setFrom(new InternetAddress(platform.getUsername(), platform.getUsername()));
        message.setSubject(platform.getTitle() != null ? platform.getTitle() : "攒批通知", "UTF-8");

        String[] recipients = platform.getReceivers().split(",");
        for (String recipient : recipients) {
            String addr = recipient.trim();
            if (!addr.isEmpty()) {
                message.addRecipient(MimeMessage.RecipientType.TO, new InternetAddress(addr));
            }
        }

        message.setText(MarkdownUtil.toHtml(content), "UTF-8", "html");
        message.saveChanges();
        return message;
    }

    private void sendMessage(Session session, MimeMessage message, NotifyPlatformPOJO platform)
            throws MessagingException {
        Transport transport = session.getTransport("smtp");
        try {
            transport.connect(platform.getHost(),
                    platform.getPort() != null ? platform.getPort() : 465,
                    platform.getUsername(), platform.getPassword());
            transport.sendMessage(message, message.getAllRecipients());
        } finally {
            try {
                transport.close();
            } catch (MessagingException e) {
                log.warn("close smtp transport failed", e);
            }
        }
        log.info("email notify sent, to={}, subject={}", message.getAllRecipients(), message.getSubject());
    }
}