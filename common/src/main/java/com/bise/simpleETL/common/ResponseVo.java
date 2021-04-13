package com.bise.simpleETL.common;

import com.bise.simpleETL.common.enums.ResponseStatusEnum;

import java.io.Serializable;

/**
 * Created with IDEA
 *
 * @author:Bise
 * @date:2021/4/2 19:55
 */
public class ResponseVo<T> implements Serializable {

    // 错误码
    private int code;
    // 错误信息
    private String error;
    // 返回数据
    private T data;

    public ResponseVo() {
        super();
    }

    public ResponseVo(ResponseStatusEnum errorEnum) {
        this(errorEnum, null);
    }

    public ResponseVo(ResponseStatusEnum errorEnum, T data) {
        this.code = errorEnum.getCode();
        this.error = errorEnum.getMessage();
        this.data = data;
    }

    public ResponseVo(int errorCode, String message) {
        this.code = errorCode;
        this.error = message;
    }

    public ResponseVo(int errorCode, String message, T data) {
        this.code = errorCode;
        this.error = message;
        this.data = data;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}

