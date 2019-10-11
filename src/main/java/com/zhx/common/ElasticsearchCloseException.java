package com.zhx.common;

/**
 * elasticsearch关闭异常
 */
public class ElasticsearchCloseException extends Exception {
    public ElasticsearchCloseException(String message) {
        super(message);
    }
}
