package com.dynamicbatch.spring.actuator;

/**
 * 运维端点统一返回结构：code / message / data。
 *
 * <p>code 语义对齐 HTTP 状态码：200 成功；400 参数非法、404 资源不存在、409 状态冲突、
 * 500 未预期异常、504 排空超时。
 */
public final class ApiResult {

    private final int code;
    private final String message;
    private final Object data;

    private ApiResult(int code, String message, Object data) {
        this.code = code;
        this.message = message;
        this.data = data;
    }

    public static ApiResult ok(Object data) {
        return new ApiResult(200, "ok", data);
    }

    public static ApiResult fail(int code, String message) {
        return new ApiResult(code, message, null);
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public Object getData() {
        return data;
    }

    @Override
    public String toString() {
        return "ApiResult{code=" + code + ", message=" + message + ", data=" + data + "}";
    }
}
