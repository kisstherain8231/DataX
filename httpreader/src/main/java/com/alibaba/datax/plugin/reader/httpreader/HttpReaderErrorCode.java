package com.alibaba.datax.plugin.reader.httpreader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * HTTP错误编码
 *
 * @author cuijianpeng
 * @date 2020/05/09 13:45
 */
public enum HttpReaderErrorCode implements ErrorCode {

    /***/
    HTTP_METHOD_NULL("HTTP_METHOD_NULL", "httpMethod为空，请检查参数后重试"),
    RESPONSE_NULL("RESPONSE_NULL", "响应值为空"),
    REQUEST_ERROR("REQUEST_ERROR", "请求错误"),
    URL_NULL("URL_NULL", "url为空，请检查参数后重试"),
    SUCCESS_CODE_NULL("SUCCESS_CODE_NULL", "successCode为空，请检查参数后重试"),
    CODE_JSON_PATH_NULL("CODE_JSON_PATH_NULL", "codeJsonPath为空，请检查参数后重试"),
    DATA_JSON_PATH_NULL("DATA_JSON_PATH_NULL", "dataJsonPath为空，请检查参数后重试"),
    COLUMN_TYPE_ERROR("COLUMN_TYPE_ERROR", "columnType配置错误"),
    PARSE_ERROR("PARSE_ERROR", "解析失败");

    private final String code;
    private final String description;

    private HttpReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
            this.description);
    }
}
