package com.dynamicbatch.common.util;

/**
 * Markdown → HTML 转换工具。
 *
 * <p>覆盖模板产出语法：h2、无序列表、行内代码、加粗、换行。
 * 非完整 markdown 解析器，不引入第三方依赖。
 */
public class MarkdownUtil {

    private MarkdownUtil() {
    }

    /**
     * 将 markdown 文本转换为简单 HTML。
     *
     * @param markdown markdown 原文
     * @return 可直接作邮件正文的 HTML 字符串
     */
    public static String toHtml(String markdown) {
        String html = markdown;

        // 转义 HTML 特殊字符
        html = html.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;");

        // ## 标题 → <h2>
        html = html.replaceAll("(?m)^## (.+)$", "<h2>$1</h2>");

        // 无序列表行 → <li>，并包裹 <ul>
        html = html.replaceAll("(?m)^- (.+)$", "<li>$1</li>");
        html = html.replaceAll("(?:<li>.*?</li>\\n?)+", "<ul>$0</ul>");

        // **加粗** → <b>
        html = html.replaceAll("\\*\\*(.+?)\\*\\*", "<b>$1</b>");

        // `行内代码` → <code>
        html = html.replaceAll("`([^`]+)`", "<code>$1</code>");

        // 连续换行 → 段落分割
        html = html.replaceAll("\\n{2,}", "</p><p>");

        return "<p>" + html + "</p>";
    }
}